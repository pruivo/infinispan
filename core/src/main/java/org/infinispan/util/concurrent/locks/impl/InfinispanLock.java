package org.infinispan.util.concurrent.locks.impl;

import org.infinispan.util.TimeService;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.locks.LockPromise;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public class InfinispanLock {

   private static final Log log = LogFactory.getLog(InfinispanLock.class);
   private static final boolean trace = log.isTraceEnabled();

   private final Queue<LockPlaceHolder> pendingRequest;
   private final ConcurrentMap<Object, LockPlaceHolder> lockOwners;
   private final AtomicReference<LockPlaceHolder> current;
   private final TimeService timeService;
   private final Runnable releaseRunnable;

   public InfinispanLock(TimeService timeService) {
      this.timeService = timeService;
      pendingRequest = new ConcurrentLinkedQueue<>();
      lockOwners = new ConcurrentHashMap<>();
      current = new AtomicReference<>(null);
      this.releaseRunnable = null;
   }

   public InfinispanLock(TimeService timeService, Runnable releaseRunnable) {
      this.timeService = timeService;
      pendingRequest = new ConcurrentLinkedQueue<>();
      lockOwners = new ConcurrentHashMap<>();
      current = new AtomicReference<>(null);
      this.releaseRunnable = releaseRunnable;
   }

   public LockPromise acquire(Object lockOwner, long time, TimeUnit timeUnit) {
      if (trace) {
         log.tracef("Acquire lock for %s. Timeout=%s (%s)", lockOwner, time, timeUnit);
      }
      LockPlaceHolder lockPlaceHolder = lockOwners.get(lockOwner);
      if (lockPlaceHolder != null) {
         if (trace) {
            log.tracef("Lock owner already exists: %s", lockPlaceHolder);
         }
         return lockPlaceHolder;
      }
      lockPlaceHolder = createLockInfo(lockOwner, time, timeUnit);
      LockPlaceHolder other = lockOwners.putIfAbsent(lockOwner, lockPlaceHolder);
      if (other != null) {
         if (trace) {
            log.tracef("Lock owner already exists: %s", other);
         }
         return other;
      }
      if (trace) {
         log.tracef("Created a new one: %s", lockPlaceHolder);
      }
      pendingRequest.add(lockPlaceHolder);
      tryAcquire();
      return lockPlaceHolder;
   }

   public void release(Object lockOwner) {
      if (trace) {
         log.tracef("Release lock for %s.", lockOwner);
      }
      LockPlaceHolder wantToRelease = lockOwners.get(lockOwner);
      if (wantToRelease == null) {
         if (trace) {
            log.tracef("%s not found!", lockOwner);
         }
         //nothing to release
         return;
      }
      final boolean released = wantToRelease.release();
      if (trace) {
         log.tracef("Release lock for %s? %s", wantToRelease, released);
      }
      LockPlaceHolder currentLocked = current.get();
      if (currentLocked == wantToRelease) {
         if (!current.compareAndSet(currentLocked, null)) {
            if (trace) {
               log.tracef("Releasing lock for %s. It is the current lock owner but another thread changed it.", lockOwner);
            }
            //another thread already released!
            return;
         } else {
            tryAcquire();
         }
      }

      if (released) {
         triggerReleased();
      }

      /*//we could change the state of the current owner if the release happens at the same time as acquisition.
      currentLocked = current.get();
      if (trace) {
         log.tracef("Releasing lock for %s. It is the current lock owner but another thread changed it.", lockOwner);
      }
      if (currentLocked != null && currentLocked.isComplete()) {
         if (current.compareAndSet(currentLocked, null)) {
            //we removed the owner and we will find another one
            tryAcquire();
         }
      }
      if (released) {
         triggerReleased();
      }*/
   }

   public boolean isEmpty() {
      //debug only
      return isFree() && lockOwners.isEmpty();
   }

   private void remove(Object lockOwner) {
      lockOwners.remove(lockOwner);
   }

   private void triggerReleased() {
      if (releaseRunnable != null) {
         releaseRunnable.run();
      }
   }

   public boolean isFree() {
      return current.get() == null && pendingRequest.isEmpty();
   }

   public Object getLockOwner() {
      LockPlaceHolder lockPlaceHolder = current.get();
      return lockPlaceHolder == null ? null : lockPlaceHolder.lockOwner;
   }

   private void tryAcquire() {
      do {
         LockPlaceHolder nextPending = pendingRequest.peek();
         if (trace) {
            log.tracef("Try acquire. Next in queue=%s. Current=%s", nextPending, current.get());
         }
         if (nextPending == null) {
            return;
         }
         if (current.compareAndSet(null, nextPending)) {
            //we set the current lock owner, so we must remove it from the queue
            pendingRequest.remove(nextPending);
            if (nextPending.acquire()) {
               if (trace) {
                  log.tracef("%s successfully acquired the lock.", nextPending);
               }
               //successfully acquired
               return;
            }
            if (trace) {
               log.tracef("%s failed to acquire (invalid state). Retrying.", nextPending);
            }
            //oh oh, probably the next in queue Timed-Out. we are going to retry with the next in queue
            current.compareAndSet(nextPending, null);
         } else {
            if (trace) {
               log.tracef("Unable to acquire. Lock is held.");
            }
            //other thread already set the current lock owner
            return;
         }
      } while (true);
   }

   private LockPlaceHolder createLockInfo(Object lockOwner, long time, TimeUnit timeUnit) {
      return new LockPlaceHolder(lockOwner, timeService.expectedEndTime(time, timeUnit));
   }

   private void onTimeout(LockPlaceHolder lockPlaceHolder) {
      //only invoked if the lock state changed to TIMED_OUT. So, it is never acquired.
      if (trace) {
         log.tracef("Timeout happened in %s", lockPlaceHolder);
      }
      triggerReleased();
   }

   private enum LockState {
      WAITING, ACQUIRED, TIMED_OUT, RELEASED
   }

   private class LockPlaceHolder implements LockPromise {

      private final AtomicReference<LockState> lockState;
      private final Object lockOwner;
      private final long timeout;
      private volatile boolean cleanup;

      private LockPlaceHolder(Object lockOwner, long timeout) {
         this.lockOwner = lockOwner;
         this.timeout = timeout;
         lockState = new AtomicReference<>(LockState.WAITING);
      }

      @Override
      public boolean isAvailable() {
         checkTimeout();
         return lockState.get() != LockState.WAITING;
      }

      @Override
      public void lock() throws InterruptedException, TimeoutException {
         checkTimeout();
         while (true) {
            LockState state = lockState.get();
            switch (state) {
               case WAITING:
                  await();
                  break;
               case ACQUIRED:
                  return; //already acquired
               case RELEASED:
                  throw new IllegalStateException("Lock already released!");
               case TIMED_OUT:
                  cleanup();
                  throw new TimeoutException("Timeout waiting for lock.");
               default:
                  throw new IllegalStateException("Unknown lock state: " + state);
            }
         }
      }

      public boolean acquire() {
         if (lockState.compareAndSet(LockState.WAITING, LockState.ACQUIRED)) {
            if (trace) {
               log.tracef("State changed for %s. %s => %s", this, LockState.WAITING, LockState.ACQUIRED);
            }
            notifyStateChanged();
         }
         return lockState.get() == LockState.ACQUIRED;
      }

      public boolean release() {
         LockState state = lockState.get();
         switch (state) {
            case WAITING:
            case ACQUIRED:
               cleanup();
               if (lockState.compareAndSet(state, LockState.RELEASED)) {
                  if (trace) {
                     log.tracef("State changed for %s. %s => %s", this, state, LockState.RELEASED);
                  }
                  notifyStateChanged();
                  return true;
               }
               break;
         }
         return false;
      }

      public boolean isComplete() {
         LockState state = lockState.get();
         return state == LockState.RELEASED || state == LockState.TIMED_OUT;
      }

      private void cleanup() {
         if (!cleanup) {
            cleanup = true;
            remove(lockOwner);
         }
      }

      private void await() throws InterruptedException {
         synchronized (this) {
            while (lockState.get() == LockState.WAITING) {
               long waitTime = timeService.remainingTime(timeout, TimeUnit.MILLISECONDS);
               if (waitTime > 0) {
                  this.wait(timeService.remainingTime(timeout, TimeUnit.MILLISECONDS));
               }
               checkTimeout();
            }
         }
      }

      private void notifyStateChanged() {
         synchronized (this) {
            this.notifyAll();
         }
      }

      private void checkTimeout() {
         if (lockState.get() != LockState.WAITING) {
            return;
         }
         if (timeService.isTimeExpired(timeout)) {
            if (lockState.compareAndSet(LockState.WAITING, LockState.TIMED_OUT)) {
               if (trace) {
                  log.tracef("State changed for %s. %s => %s", this, LockState.WAITING, LockState.TIMED_OUT);
               }
               onTimeout(this); //we release before notify (notify can check the remote executor and we need to be ready)
               notifyStateChanged();
            }
         }
      }

      @Override
      public String toString() {
         return "LockPlaceHolder{" +
               "lockState=" + lockState.get() +
               ", lockOwner=" + lockOwner +
               '}';
      }
   }
}
