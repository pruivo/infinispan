package org.infinispan.util.concurrent.locks.impl;

import org.infinispan.factories.annotations.Inject;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.locks.CancellableLockPromise;
import org.infinispan.util.concurrent.locks.LockContainerV8;
import org.infinispan.util.concurrent.locks.LockManagerV8;
import org.infinispan.util.concurrent.locks.LockPromise;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public class DefaultLockManager implements LockManagerV8 {

   private LockContainerV8 container;

   @Inject
   public void inject(LockContainerV8 container) {
      this.container = container;
   }

   @Override
   public LockPromise lock(Object key, Object lockOwner, long time, TimeUnit unit) {
      return container.acquire(key, lockOwner, time, unit);
   }

   @Override
   public LockPromise lockAll(Collection<?> keys, Object lockOwner, long time, TimeUnit unit) {
      final Set<Object> uniqueKeys = new HashSet<>(keys);
      if (uniqueKeys.isEmpty()) {
         return null;
      } else if (uniqueKeys.size() == 1) {
         return lock(uniqueKeys.iterator().next(), lockOwner, time, unit);
      }
      final CompositeLockPromise compositeLockPromise = new CompositeLockPromise(uniqueKeys.size());
      synchronized (this) {
         for (Object key : uniqueKeys) {
            compositeLockPromise.addLock(container.acquire(key, lockOwner, time, unit));
         }
      }
      return compositeLockPromise;
   }

   @Override
   public void unlock(Object key, Object lockOwner) {
      container.release(key, lockOwner);
   }

   @Override
   public void unlockAll(Collection<?> keys, Object lockOwner) {
      if (keys.isEmpty()) {
         return;
      }
      for (Object key : keys) {
         container.release(key, lockOwner);
      }
   }

   @Override
   public boolean ownsLock(Object key, Object lockOwner) {
      Object currentOwner = getOwner(key);
      return currentOwner != null && currentOwner.equals(lockOwner);
   }

   @Override
   public boolean isLocked(Object key) {
      return getOwner(key) != null;
   }

   @Override
   public Object getOwner(Object key) {
      InfinispanLock lock = container.getLock(key);
      return lock == null ? null : lock.getLockOwner();
   }

   @Override
   public String printLockInfo() {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public int getNumberOfLocksHeld() {
      return container.getNumLocksHeld();
   }

   @Override
   public int getLockId(Object key) {
      InfinispanLock lock = container.getLock(key);
      return lock == null ? 0 : lock.hashCode();
   }

   private static class CompositeLockPromise implements LockPromise, Runnable {

      private final List<CancellableLockPromise> lockPromiseList;
      private final AtomicBoolean notify;
      private volatile Runnable availableRunnable;

      private CompositeLockPromise(int size) {
         lockPromiseList = new ArrayList<>(size);
         notify = new AtomicBoolean(false);
      }

      public void addLock(CancellableLockPromise lockPromise) {
         lockPromiseList.add(lockPromise);
         lockPromise.setAvailableRunnable(this);
      }

      @Override
      public boolean isAvailable() {
         for (LockPromise lockPromise : lockPromiseList) {
            if (!lockPromise.isAvailable()) {
               return false;
            }
         }
         return true;
      }

      @Override
      public void lock() throws InterruptedException, TimeoutException {
         Exception exception = null;
         ExceptionType exceptionType = ExceptionType.NONE;
         for (CancellableLockPromise lockPromise : lockPromiseList) {
            try {
               lockPromise.lock();
            } catch (InterruptedException e) {
               if (exception == null) {
                  exception = e;
                  exceptionType = ExceptionType.INTERRUPTED;
               }
            } catch (TimeoutException e) {
               if (exception == null) {
                  exception = e;
                  exceptionType = ExceptionType.TIMEOUT;
               }
            } catch (RuntimeException e) {
               if (exception == null) {
                  exception = e;
                  exceptionType = ExceptionType.RUNTIME;
               }
            }
         }
         if (exception != null) {
            for (CancellableLockPromise lockPromise : lockPromiseList) {
               lockPromise.cancel();
            }
            switch (exceptionType) {
               case INTERRUPTED:
                  throw (InterruptedException) exception;
               case TIMEOUT:
                  throw (TimeoutException) exception;
               case RUNTIME:
                  throw (RuntimeException) exception;
               default:
                  break;
            }
         }
      }

      @Override
      public void setAvailableRunnable(Runnable runnable) {
         this.availableRunnable = runnable;
         notifyAvailable();
      }

      @Override
      public void run() {
         notifyAvailable();
      }

      private void notifyAvailable() {
         Runnable runnable = availableRunnable;
         if (isAvailable() && runnable != null && notify.compareAndSet(false, true)) {
            runnable.run();
         }
      }

      private enum ExceptionType {
         RUNTIME, INTERRUPTED, TIMEOUT, NONE
      }

   }
}
