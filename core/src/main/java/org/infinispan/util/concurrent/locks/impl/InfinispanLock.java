package org.infinispan.util.concurrent.locks.impl;

import org.infinispan.commons.util.Notifier;
import org.infinispan.util.TimeService;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.locks.DeadlockChecker;
import org.infinispan.util.concurrent.locks.DeadlockDetectedException;
import org.infinispan.util.concurrent.locks.ExtendedLockPromise;
import org.infinispan.util.concurrent.locks.LockListener;
import org.infinispan.util.concurrent.locks.LockState;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

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
   private final AtomicReferenceFieldUpdater<InfinispanLock, LockPlaceHolder> fieldUpdater;
   private final Runnable releaseRunnable;
   private volatile TimeService timeService;
   private volatile LockPlaceHolder current;

   public InfinispanLock(TimeService timeService) {
      this.timeService = timeService;
      pendingRequest = new ConcurrentLinkedQueue<>();
      lockOwners = new ConcurrentHashMap<>();
      current = null;
      fieldUpdater = AtomicReferenceFieldUpdater.newUpdater(InfinispanLock.class, LockPlaceHolder.class, "current");
      releaseRunnable = null;
   }

   public InfinispanLock(TimeService timeService, Runnable releaseRunnable) {
      this.timeService = timeService;
      pendingRequest = new ConcurrentLinkedQueue<>();
      lockOwners = new ConcurrentHashMap<>();
      current = null;
      fieldUpdater = AtomicReferenceFieldUpdater.newUpdater(InfinispanLock.class, LockPlaceHolder.class, "current");
      this.releaseRunnable = releaseRunnable;
   }

   /**
    * Tests purpose only!
    */
   public void setTimeService(TimeService timeService) {
      if (timeService != null) {
         this.timeService = timeService;
      }
   }

   public ExtendedLockPromise acquire(Object lockOwner, long time, TimeUnit timeUnit) {
      Objects.requireNonNull(lockOwner, "Lock Owner should be non-null");
      Objects.requireNonNull(timeUnit, "Time Unit should be non-null");
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
      Objects.requireNonNull(lockOwner, "Lock Owner should be non-null");
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
      final boolean released = wantToRelease.setReleased();
      if (trace) {
         log.tracef("Release lock for %s? %s", wantToRelease, released);
      }
      LockPlaceHolder currentLocked = current;
      if (currentLocked == wantToRelease) {
         if (!casRelease(currentLocked)) {
            if (trace) {
               log.tracef("Releasing lock for %s. It is the current lock owner but another thread changed it.", lockOwner);
            }
            //another thread already released!
         } else {
            tryAcquire();
         }
      }
   }

   public boolean isEmpty() {
      //debug only
      return isFree() && lockOwners.isEmpty();
   }

   public boolean isFree() {
      return current == null && pendingRequest.isEmpty();
   }

   public Object getLockOwner() {
      LockPlaceHolder lockPlaceHolder = current;
      return lockPlaceHolder == null ? null : lockPlaceHolder.lockOwner;
   }

   public boolean isLocked() {
      return current != null;
   }

   public void deadlockCheck() {
      LockPlaceHolder holder = current;
      if (holder != null) {
         for (LockPlaceHolder pending : pendingRequest) {
            pending.checkDeadlock(holder.lockOwner);
         }
      }
   }

   public boolean containsLockOwner(Object lockOwner) {
      LockPlaceHolder holder = current;
      if (holder != null && holder.lockOwner.equals(lockOwner)) {
         return true;
      }
      if (pendingRequest.isEmpty()) {
         return false;
      }

      for(LockPlaceHolder pendingHolder : pendingRequest) {
         if (pendingHolder.lockOwner.equals(lockOwner)) {
            return true;
         }
      }
      holder = current;
      return holder != null && holder.lockOwner.equals(lockOwner);
   }

   private void onCanceled(LockPlaceHolder canceled) {
      if (trace) {
         log.tracef("Release lock for %s. It was canceled.", canceled.lockOwner);
      }
      LockPlaceHolder wantToRelease = lockOwners.get(canceled.lockOwner);
      if (wantToRelease == null) {
         if (trace) {
            log.tracef("%s not found!", canceled.lockOwner);
         }
         //nothing to release
         return;
      }
      LockPlaceHolder currentLocked = current;
      if (currentLocked == wantToRelease) {
         if (!casRelease(currentLocked)) {
            if (trace) {
               log.tracef("Releasing lock for %s. It is the current lock owner but another thread changed it.", canceled.lockOwner);
            }
            //another thread already released!
         } else {
            tryAcquire();
         }
      }
   }

   private boolean casAcquire(LockPlaceHolder lockPlaceHolder) {
      return fieldUpdater.compareAndSet(this, null, lockPlaceHolder);
   }

   private boolean casRelease(LockPlaceHolder lockPlaceHolder) {
      return fieldUpdater.compareAndSet(this, lockPlaceHolder, null);
   }

   private void remove(Object lockOwner) {
      lockOwners.remove(lockOwner);
   }

   private void triggerReleased() {
      if (releaseRunnable != null) {
         releaseRunnable.run();
      }
   }

   private void tryAcquire() {
      do {
         LockPlaceHolder nextPending = pendingRequest.peek();
         if (trace) {
            log.tracef("Try acquire. Next in queue=%s. Current=%s", nextPending, current);
         }
         if (nextPending == null) {
            return;
         }
         if (casAcquire(nextPending)) {
            //we set the current lock owner, so we must remove it from the queue
            pendingRequest.remove(nextPending);
            if (nextPending.setAvailable()) {
               if (trace) {
                  log.tracef("%s successfully acquired the lock.", nextPending);
               }
               //successfully acquired
               deadlockCheck();
               return;
            }
            if (trace) {
               log.tracef("%s failed to acquire (invalid state). Retrying.", nextPending);
            }
            //oh oh, probably the next in queue Timed-Out. we are going to retry with the next in queue
            casRelease(nextPending);
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

   private class LockPlaceHolder implements ExtendedLockPromise, Notifier.Invoker<LockListener> {

      private final AtomicReferenceFieldUpdater<LockPlaceHolder, LockState> stateUpdater;

      private final Object lockOwner;
      private final long timeout;
      private final AtomicBoolean cleanup;
      private final Notifier<LockListener> notifier;
      private volatile LockState lockState;
      private volatile DeadlockChecker deadlockChecker;

      private LockPlaceHolder(Object lockOwner, long timeout) {
         this.lockOwner = lockOwner;
         this.timeout = timeout;
         lockState = LockState.WAITING;
         stateUpdater = AtomicReferenceFieldUpdater.newUpdater(LockPlaceHolder.class, LockState.class, "lockState");
         cleanup = new AtomicBoolean(false);
         notifier = new Notifier<>(this);
      }

      @Override
      public boolean isAvailable() {
         checkTimeout();
         return lockState != LockState.WAITING;
      }

      @Override
      public void lock() throws InterruptedException, TimeoutException {
         while (true) {
            switch (lockState) {
               case WAITING:
                  checkTimeout();
                  notifier.await(timeService.remainingTime(timeout, TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
                  break;
               case AVAILABLE:
                  if (casState(LockState.AVAILABLE, LockState.ACQUIRED)) {
                     return; //acquired!
                  }
                  break;
               case ACQUIRED:
                  return; //acquired!
               case RELEASED:
                  throw new IllegalStateException("Lock already released!");
               case TIMED_OUT:
                  cleanup();
                  throw new TimeoutException("Timeout waiting for lock.");
               case DEADLOCKED:
                  cleanup();
                  throw new DeadlockDetectedException("DeadLock detected");
               default:
                  throw new IllegalStateException("Unknown lock state: " + lockState);
            }
         }
      }

      @Override
      public void addListener(LockListener listener) {
         notifier.add(listener);
      }

      @Override
      public void cancel(LockState state) {
         checkValidCancelState(state);
         out:
         do {
            LockState currentState = lockState;
            switch (currentState) {
               case WAITING:
                  if (casState(LockState.WAITING, state)) {
                     notifyListeners();
                     break out;
                  }
                  break;
               case ACQUIRED: //no-op, a thread is inside the critical section.
               case TIMED_OUT:
               case DEADLOCKED:
               case RELEASED:
                  return; //no-op, the lock is in final state.
               default:
                  if (casState(currentState, state)) {
                     break out;
                  }

            }
         } while (true);
         onCanceled(this);
      }

      @Override
      public Object getRequestor() {
         return lockOwner;
      }

      @Override
      public Object getOwner() {
         LockPlaceHolder owner = current;
         return owner != null ? owner.lockOwner : null;
      }

      @Override
      public String toString() {
         return "LockPlaceHolder{" +
               "lockState=" + lockState +
               ", lockOwner=" + lockOwner +
               '}';
      }

      @Override
      public void invoke(LockListener invoker) {
         LockState state = lockState;
         switch (state) {
            case WAITING:
               throw new IllegalStateException("WAITING is not a valid state to invoke the listener");
            case ACQUIRED:
            case RELEASED:
               invoker.onEvent(LockState.AVAILABLE);
               break;
            default:
               invoker.onEvent(state);
               break;
         }
      }

      @Override
      public void setDeadlockChecker(DeadlockChecker deadlockChecker) {
         this.deadlockChecker = deadlockChecker;
         LockPlaceHolder currentHolder = current;
         if (currentHolder != null) {
            checkDeadlock(currentHolder.lockOwner);
         }
      }

      private void checkValidCancelState(LockState state) {
         switch (state) {
            case WAITING:
            case AVAILABLE:
            case ACQUIRED:
            case RELEASED:
               throw new IllegalArgumentException("LockState " + state + " is not valid to cancel.");
         }
      }

      private void checkDeadlock(Object currentOwner) {
         DeadlockChecker checker = deadlockChecker;
         checkTimeout(); //check timeout before checking the deadlock. check deadlock are more expensive.
         if (checker != null && //we have a deadlock checker installed
               lockState == LockState.WAITING && //we are waiting for a lock
               !lockOwner.equals(currentOwner) && //needed? just to be safe
               checker.deadlockDetected(lockOwner, currentOwner) && //deadlock has been detected!
               casState(LockState.WAITING, LockState.DEADLOCKED)) { //state could have been changed to available or timed_out
            onCanceled(this);
            notifyListeners();
         }
      }

      private boolean setAvailable() {
         if (casState(LockState.WAITING, LockState.AVAILABLE)) {
            notifyListeners();
         }
         LockState state = lockState;
         return state == LockState.AVAILABLE || state == LockState.ACQUIRED;
      }

      private boolean setReleased() {
         do {
            LockState state = lockState;
            switch (state) {
               case WAITING:
               case AVAILABLE:
               case ACQUIRED:
                  if (casState(state, LockState.RELEASED)) {
                     cleanup();
                     notifyListeners();
                     return true;
                  }
                  break;
               case TIMED_OUT:
               case DEADLOCKED:
                  if (casState(state, LockState.RELEASED)) {
                     cleanup();
                     return true;
                  }
                  break;
               default:
                  return false;
            }
         } while (true);
      }

      private boolean casState(LockState expect, LockState update) {
         boolean updated = stateUpdater.compareAndSet(this, expect, update);
         if (updated && trace) {
            log.tracef("State changed for %s. %s => %s", this, expect, update);
         }
         return updated;
      }

      private void cleanup() {
         if (cleanup.compareAndSet(false, true)) {
            remove(lockOwner);
            triggerReleased();
         }
      }

      private void checkTimeout() {
         if (lockState == LockState.WAITING &&
               timeService.isTimeExpired(timeout) &&
               casState(LockState.WAITING, LockState.TIMED_OUT)) {
            onCanceled(this);
            notifyListeners();
         }

      }

      private void notifyListeners() {
         if (lockState != LockState.WAITING) {
            notifier.fireListener();
         }
      }
   }
}
