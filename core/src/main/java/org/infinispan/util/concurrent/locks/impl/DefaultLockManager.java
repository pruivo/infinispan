package org.infinispan.util.concurrent.locks.impl;

import org.infinispan.commons.util.Notifier;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.jmx.annotations.DataType;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.locks.DeadlockChecker;
import org.infinispan.util.concurrent.locks.DeadlockDetectedException;
import org.infinispan.util.concurrent.locks.ExtendedLockPromise;
import org.infinispan.util.concurrent.locks.LockContainer;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.concurrent.locks.LockPromise;
import org.infinispan.util.concurrent.locks.LockState;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
@MBean(objectName = "LockManager", description = "Manager that handles MVCC locks for entries")
public class DefaultLockManager implements LockManager {

   private static final Log log = LogFactory.getLog(DefaultLockManager.class);
   private static final boolean trace = log.isTraceEnabled();
   protected LockContainer container;
   protected Configuration configuration;
   protected ScheduledExecutorService scheduler;

   @Inject
   public void inject(LockContainer container, Configuration configuration,
                      @ComponentName(KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR) ScheduledExecutorService executorService) {
      this.container = container;
      this.configuration = configuration;
      this.scheduler = executorService;
   }


   @Override
   public LockPromise lock(Object key, Object lockOwner, long time, TimeUnit unit) {
      if (trace) {
         log.tracef("Lock key=%s for owner=%s. timeout=%s (%s)", key, lockOwner, time, unit);
      }
      LockPromise promise = container.acquire(key, lockOwner, time, unit);
      scheduleLockPromise(promise, time, unit);
      return promise;
   }

   @Override
   public LockPromise lockAll(Collection<?> keys, Object lockOwner, long time, TimeUnit unit) {
      final Set<Object> uniqueKeys = new HashSet<>(keys);
      if (trace) {
         log.tracef("Lock all keys=%s for owner=%s. timeout=%s (%s)", uniqueKeys, lockOwner, time, unit);
      }
      if (uniqueKeys.isEmpty()) {
         return LockPromise.NO_OP;
      } else if (uniqueKeys.size() == 1) {
         return lock(uniqueKeys.iterator().next(), lockOwner, time, unit);
      }
      final CompositeLockPromise compositeLockPromise = new CompositeLockPromise(uniqueKeys.size());
      synchronized (this) {
         for (Object key : uniqueKeys) {
            compositeLockPromise.addLock(container.acquire(key, lockOwner, time, unit));
         }
      }
      compositeLockPromise.markListAsFinal();
      scheduleLockPromise(compositeLockPromise, time, unit);
      return compositeLockPromise;
   }

   @Override
   public void unlock(Object key, Object lockOwner) {
      if (trace) {
         log.tracef("Release lock for key=%s. owner=%s", key, lockOwner);
      }
      container.release(key, lockOwner);
   }

   @Override
   public void unlockAll(Collection<?> keys, Object lockOwner) {
      if (trace) {
         log.tracef("Release locks for keys=%s. owner=%s", keys, lockOwner);
      }
      if (keys.isEmpty()) {
         return;
      }
      for (Object key : keys) {
         container.release(key, lockOwner);
      }
   }

   @Override
   public void unlockAll(InvocationContext context) {
      unlockAll(context.getLockedKeys(), context.getLockOwner());
      context.clearLockedKeys();
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
   @ManagedAttribute(description = "The number of exclusive locks that are held.", displayName = "Number of locks held")
   public int getNumberOfLocksHeld() {
      return container.getNumLocksHeld();
   }

   @ManagedAttribute(description = "The concurrency level that the MVCC Lock Manager has been configured with.", displayName = "Concurrency level", dataType = DataType.TRAIT)
   public int getConcurrencyLevel() {
      return configuration.locking().concurrencyLevel();
   }

   @ManagedAttribute(description = "The number of exclusive locks that are available.", displayName = "Number of locks available")
   public int getNumberOfLocksAvailable() {
      return container.size() - container.getNumLocksHeld();
   }

   @Override
   public long getDefaultTimeoutMillis() {
      return configuration.locking().lockAcquisitionTimeout();
   }

   private void scheduleLockPromise(LockPromise promise, long time, TimeUnit unit) {
      if (!promise.isAvailable() && time > 0 && scheduler != null) {
         final ScheduledFuture<?> future = scheduler.schedule(promise::isAvailable, time, unit);
         promise.addListener(state -> future.cancel(false));
      }
   }

   private static class CompositeLockPromise implements LockPromise, LockPromise.Listener, Notifier.Invoker<LockPromise.Listener> {

      private final List<ExtendedLockPromise> lockPromiseList;
      private final Notifier<Listener> notifier;
      private final AtomicReferenceFieldUpdater<CompositeLockPromise, LockState> stateUpdater;
      private volatile LockState lockState = LockState.AVAILABLE;

      private CompositeLockPromise(int size) {
         lockPromiseList = new ArrayList<>(size);
         notifier = new Notifier<>(this);
         stateUpdater = AtomicReferenceFieldUpdater.newUpdater(CompositeLockPromise.class, LockState.class, "lockState");
      }

      public void addLock(ExtendedLockPromise lockPromise) {
         lockPromiseList.add(lockPromise);
      }

      public void markListAsFinal() {
         for (LockPromise lockPromise : lockPromiseList) {
            lockPromise.addListener(this);
         }
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
         for (ExtendedLockPromise lockPromise : lockPromiseList) {
            try {
               //we still need to invoke lock in all the locks.
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
            } catch (DeadlockDetectedException e) {
               if (exception == null) {
                  exception = e;
                  exceptionType = ExceptionType.DEADLOCK;
               }
            } catch (RuntimeException e) {
               if (exception == null) {
                  exception = e;
                  exceptionType = ExceptionType.RUNTIME;
               }
            }
         }
         if (exception != null) {
            switch (exceptionType) {
               case INTERRUPTED:
                  throw (InterruptedException) exception;
               case TIMEOUT:
                  throw (TimeoutException) exception;
               case RUNTIME:
                  throw (RuntimeException) exception;
               case DEADLOCK:
                  throw (DeadlockDetectedException) exception;
               default:
                  break;
            }
         }
      }

      @Override
      public void addListener(Listener listener) {
         notifier.add(listener);
      }

      @Override
      public void setDeadlockChecker(DeadlockChecker deadlockChecker) {
         for (LockPromise lockPromise : lockPromiseList) {
            lockPromise.setDeadlockChecker(deadlockChecker);
         }
      }

      @Override
      public void onEvent(LockState state) {
         if (state != LockState.AVAILABLE && stateUpdater.compareAndSet(this, LockState.AVAILABLE, state)) {
            for (ExtendedLockPromise lockPromise : lockPromiseList) {
               lockPromise.cancel(state);
            }
         }
         if (isAvailable()) {
            notifier.fireListener();
         }
      }

      @Override
      public void invoke(Listener invoker) {
         invoker.onEvent(lockState);
      }

      private enum ExceptionType {
         RUNTIME, INTERRUPTED, TIMEOUT, DEADLOCK, NONE
      }

   }
}
