package org.infinispan.util.concurrent.locks.impl;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.locks.CancellableLockPromise;
import org.infinispan.util.concurrent.locks.LockContainer;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.concurrent.locks.LockPromise;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public class DefaultLockManager implements LockManager {

   private static final Log log = LogFactory.getLog(DefaultLockManager.class);
   private static final boolean trace = log.isTraceEnabled();

   private ScheduledExecutorService scheduler;
   protected LockContainer container;
   protected Configuration configuration;

   @Inject
   public void inject(LockContainer container, Configuration configuration,
                      @ComponentName(KnownComponentNames.EXPIRATION_SCHEDULED_EXECUTOR) ScheduledExecutorService executorService) {
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
   public int getConcurrencyLevel() {
      return configuration.locking().concurrencyLevel();
   }

   @Override
   public int getNumberOfLocksAvailable() {
      return container.size() - container.getNumLocksHeld();
   }

   @Override
   public long getDefaultTimeoutMillis() {
      return configuration.locking().lockAcquisitionTimeout();
   }

   private void scheduleLockPromise(LockPromise promise, long time, TimeUnit unit) {
      if (!promise.isAvailable() && scheduler != null) {
         scheduler.schedule(promise::isAvailable, time, unit);
      }
   }

   private static class CompositeLockPromise implements LockPromise, LockPromise.Listener {

      private final List<CancellableLockPromise> lockPromiseList;
      @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
      private final CopyOnWriteArrayList<Listener> listeners;
      private volatile boolean acquired = true;

      private CompositeLockPromise(int size) {
         lockPromiseList = new ArrayList<>(size);
         listeners = new CopyOnWriteArrayList<>();
      }

      public void addLock(CancellableLockPromise lockPromise) {
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
            lockPromiseList.forEach(org.infinispan.util.concurrent.locks.CancellableLockPromise::cancel);
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
      public void addListener(Listener listener) {
         listeners.add(listener);
         notifyAvailable();
      }

      @Override
      public void onEvent(boolean acquired) {
         if (!acquired) {
            this.acquired = false;
         }
         notifyAvailable();
      }

      private void notifyAvailable() {
         if (isAvailable()) {
            listeners.removeIf(listener -> {
               listener.onEvent(acquired);
               return true;
            });
         }
      }

      private enum ExceptionType {
         RUNTIME, INTERRUPTED, TIMEOUT, NONE
      }

   }
}
