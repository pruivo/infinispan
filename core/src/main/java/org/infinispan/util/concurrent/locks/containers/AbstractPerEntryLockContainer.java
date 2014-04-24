package org.infinispan.util.concurrent.locks.containers;

import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.commons.util.ByRef;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.concurrent.jdk8backported.EquivalentConcurrentHashMapV8;
import org.infinispan.util.concurrent.locks.containers.wrappers.LockHolder;
import org.infinispan.util.concurrent.locks.containers.wrappers.LockWrapper;
import org.infinispan.util.logging.Log;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.infinispan.commons.util.Util.toStr;

/**
 * An abstract lock container that creates and maintains a new lock per entry
 *
 * @author Manik Surtani
 * @since 4.0
 */
public abstract class AbstractPerEntryLockContainer<L extends LockWrapper> extends AbstractLockContainer<L> {

   // We specifically need a CHMV8, to be able to use methods like computeIfAbsent, computeIfPresent and compute
   private final EquivalentConcurrentHashMapV8<Object, InternalLockWrapper> locks;

   protected AbstractPerEntryLockContainer(int concurrencyLevel, Equivalence<Object> keyEquivalence) {
      locks = new EquivalentConcurrentHashMapV8<Object, InternalLockWrapper>(
            16, concurrencyLevel, keyEquivalence, AnyEquivalence.getInstance());
   }

   @Override
   public final int getNumLocksHeld() {
      return locks.size();
   }

   @Override
   public final int size() {
      return locks.size();
   }

   @Override
   public final boolean acquireLock(final Object lockOwner, final Object key, final long timeout, final TimeUnit unit) throws InterruptedException {
      final ByRef<Boolean> lockAcquired = ByRef.create(Boolean.FALSE);
      InternalLockWrapper lock = locks.compute(key, new EquivalentConcurrentHashMapV8.BiFun<Object, InternalLockWrapper, InternalLockWrapper>() {
         @Override
         public InternalLockWrapper apply(Object key, InternalLockWrapper lock) {
            // This happens atomically in the CHM
            if (lock == null) {
               Log log = getLog();
               if (log.isTraceEnabled())
                  log.tracef("Creating and acquiring new lock instance for key %s", toStr(key));

               lock = new InternalLockWrapper(newLockWrapper());
               // Since this is a new lock, it is certainly uncontended.
               lock.lock(lockOwner);
               lock.getReferenceCounter().incrementAndGet();
               lockAcquired.set(Boolean.TRUE);
               return lock;
            }

            // No need to worry about concurrent updates - there can't be a release in progress at the same time
            int refCount = lock.getReferenceCounter().incrementAndGet();
            if (refCount <= 1 && lock.isEmpty()) {
               //now, it is possible to be 1 if the lock is pre-acquired before!
               throw new IllegalStateException("Lock " + key + " acquired although it should have been removed: " + lock);
            }
            return lock;
         }
      });

      if (!lockAcquired.get()) {
         // We retrieved a lock that was already present,
         lockAcquired.set(lock.tryLock(lockOwner, timeout, unit));
      }

      if (lockAcquired.get())
         return true;
      else {
         getLog().tracef("Timed out attempting to acquire lock for key %s after %s", key, Util.prettyPrintTime(timeout, unit));

         // We didn't acquire the lock, but we still incremented the reference count.
         // We may need to delete the entry if the owner thread released it just after we timed out.
         // We use an atomic operation here as another thread might be trying to increment the ref count
         // at the same time (otherwise it would make the acquire function at the beginning more complicated).
         locks.computeIfPresent(key, new EquivalentConcurrentHashMapV8.BiFun<Object, InternalLockWrapper, InternalLockWrapper>() {
            @Override
            public InternalLockWrapper apply(Object key, InternalLockWrapper lock) {
               // This will happen atomically in the CHM
               // We have a reference, so value can't be null
               boolean remove = lock.getReferenceCounter().decrementAndGet() == 0;
               return remove ? null : lock;
            }
         });
         return false;
      }
   }

   @Override
   public final void releaseLock(final Object lockOwner, Object key) {
      locks.computeIfPresent(key, new EquivalentConcurrentHashMapV8.BiFun<Object, InternalLockWrapper, InternalLockWrapper>() {
         @Override
         public InternalLockWrapper apply(Object key, InternalLockWrapper lock) {
            // This will happen atomically in the CHM
            // We have a reference, so value can't be null
            Log log = getLog();
            if (log.isTraceEnabled())
               log.tracef("Unlocking lock instance for key %s", toStr(key));

            lock.unlock(lockOwner);

            int refCount = lock.getReferenceCounter().decrementAndGet();
            boolean remove = refCount == 0;
            if (refCount < 0) {
               throw new IllegalStateException("Negative reference count for lock " + key + ": " + lock);
            }

            // Ok, unlock was successful.  If the unlock was not successful, an exception will propagate and the entry will not be changed.
            return remove ? null : lock;
         }
      });
   }

   @Override
   public final int getLockId(Object key) {
      L lock = getLock(key);
      return lock == null ? -1 : System.identityHashCode(lock);
   }

   @Override
   public String toString() {
      return "AbstractPerEntryLockContainer{" +
            "locks=" + locks +
            '}';
   }

   protected abstract L newLockWrapper();

   @Override
   protected final L getLock(Object key) {
      InternalLockWrapper lock = locks.get(key);
      return lock == null ? null : lock.lockWrapper;
   }

   @Override
   public boolean isQueuesEmpty() {
      for (InternalLockWrapper lockWrapper : locks.values()) {
         if (!lockWrapper.isEmpty()) {
            return false;
         }
      }
      return true;
   }

   @Override
   protected LockHolder addLockHolder(final Object key, final Object lockOwner) {
      final ByRef<LockHolder> lockHolder = new ByRef<LockHolder>(null);
      locks.compute(key, new EquivalentConcurrentHashMapV8.BiFun<Object, InternalLockWrapper, InternalLockWrapper>() {
         @Override
         public InternalLockWrapper apply(Object o, InternalLockWrapper lock) {
            if (lock == null) {
               lock = new InternalLockWrapper(newLockWrapper());
            }
            lockHolder.set(lock.add(key, lockOwner));
            return lock;
         }
      });
      return lockHolder.get();
   }

   @Override
   protected void lockHolderTimeout(final LockHolder lockHolder) {
      locks.computeIfPresent(lockHolder.getKey(), new EquivalentConcurrentHashMapV8.BiFun<Object, InternalLockWrapper, InternalLockWrapper>() {
         @Override
         public InternalLockWrapper apply(Object o, InternalLockWrapper lock) {
            lock.remove(lockHolder);
            int refCount = lock.getReferenceCounter().get();
            boolean remove = refCount == 0;
            if (refCount < 0) {
               throw new IllegalStateException("Negative reference count for lock " + lockHolder.getKey() + ": " + lock);
            }

            // Ok, unlock was successful.  If the unlock was not successful, an exception will propagate and the entry will not be changed.
            return remove ? null : lock;
         }
      });
   }

   private class InternalLockWrapper implements LockWrapper {

      private final L lockWrapper;
      private final AtomicInteger referenceCounter = new AtomicInteger(0);

      private InternalLockWrapper(L lockWrapper) {
         this.lockWrapper = lockWrapper;
      }

      public AtomicInteger getReferenceCounter() {
         return referenceCounter;
      }

      @Override
      public LockHolder add(Object key, Object lockOwner) {
         return lockWrapper.add(key, lockOwner);
      }

      @Override
      public Object getOwner() {
         return lockWrapper.getOwner();
      }

      @Override
      public boolean isLocked() {
         return lockWrapper.isLocked();
      }

      @Override
      public boolean tryLock(Object lockOwner, long timeout, TimeUnit unit) throws InterruptedException {
         return lockWrapper.tryLock(lockOwner, timeout, unit);
      }

      @Override
      public void lock(Object lockOwner) {
         lockWrapper.lock(lockOwner);
      }

      @Override
      public void unlock(Object owner) {
         lockWrapper.unlock(owner);
      }

      @Override
      public void remove(LockHolder lockHolder) {
         lockWrapper.remove(lockHolder);
      }

      @Override
      public boolean isEmpty() {
         return lockWrapper.isEmpty();
      }
   }

}
