package org.infinispan.util.concurrent.locks.impl;

import org.infinispan.util.TimeService;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.locks.LockPromise;

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

   private final Queue<LockInfo> pendingRequest;
   private final ConcurrentMap<Object, LockInfo> lockOwners;
   private final AtomicReference<LockInfo> current;
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
      LockInfo lockInfo = lockOwners.get(lockOwner);
      if (lockInfo != null) {
         return lockInfo;
      }
      lockInfo = createLockInfo(lockOwner, time, timeUnit);
      LockInfo other = lockOwners.putIfAbsent(lockOwner, lockInfo);
      if (other != null) {
         return other;
      }
      pendingRequest.add(lockInfo);
      tryAcquire();
      return lockInfo;
   }

   public void release(Object lockOwner) {
      LockInfo wantToRelease = lockOwners.remove(lockOwner);
      if (wantToRelease == null) {
         //nothing to release
         return;
      }
      boolean released = false;
      LockInfo currentLocked = current.get();
      if (currentLocked == wantToRelease) {
         if (!current.compareAndSet(currentLocked, null)) {
            //another thread already released!
            return;
         }
         released = wantToRelease.release();
         //no need to iterate
         tryAcquire();
         if (released) {
            triggerReleased();
         }
         return;
      }
      for (LockInfo lockInfo : pendingRequest) {
         if (lockInfo.lockOwner.equals(lockOwner)) {
            released = lockInfo.release();
            break;
         }
      }

      //we could change the state of the current owner if the release happens at the same time as acquisition.
      currentLocked = current.get();
      if (currentLocked != null && currentLocked.isComplete()) {
         if (current.compareAndSet(currentLocked, null)) {
            //we removed the owner and we will find another one
            tryAcquire();
         }
      }
      if (released) {
         triggerReleased();
      }
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
      LockInfo lockInfo = current.get();
      return lockInfo == null ? null : lockInfo.lockOwner;
   }

   private void tryAcquire() {
      do {
         LockInfo nextPending = pendingRequest.peek();
         if (nextPending == null) {
            return;
         }
         if (current.compareAndSet(null, nextPending)) {
            //we set the current lock owner, so we must remove it from the queue
            pendingRequest.remove();
            if (nextPending.acquire()) {
               //successfully acquired
               return;
            }
            //oh oh, probably the next in queue Timed-Out. we are going to retry with the next in queue
            current.set(null);
         } else {
            //other thread already set the current lock owner
            return;
         }
      } while (true);
   }

   private LockInfo createLockInfo(Object lockOwner, long time, TimeUnit timeUnit) {
      return new LockInfo(lockOwner, timeService.expectedEndTime(time, timeUnit));
   }

   private void onTimeout(LockInfo lockInfo) {
      lockOwners.remove(lockInfo);
      LockInfo currentLocked = current.get();
      if (currentLocked == lockInfo) {
         if (!current.compareAndSet(currentLocked, null)) {
            //another thread already released!
            triggerReleased();
            return;
         }
         tryAcquire();
      }
      triggerReleased();
   }

   private enum LockState {
      WAITING, ACQUIRED, TIMED_OUT, RELEASED
   }

   private class LockInfo implements LockPromise {

      private final AtomicReference<LockState> lockState;
      private final Object lockOwner;
      private final long timeout;

      private LockInfo(Object lockOwner, long timeout) {
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
               throw new TimeoutException("Timeout waiting for lock.");
            default:
               throw new IllegalStateException("Unknown lock state: " + state);
         }
      }

      public boolean acquire() {
         if (lockState.compareAndSet(LockState.WAITING, LockState.ACQUIRED)) {
            notifyStateChanged();
         }
         return lockState.get() == LockState.ACQUIRED;
      }

      public boolean release() {
         LockState state = lockState.get();
         switch (state) {
            case WAITING:
            case ACQUIRED:
               if (lockState.compareAndSet(state, LockState.RELEASED)) {
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
               onTimeout(this); //we release before notify (notify can check the remote executor and we need to be ready)
               notifyStateChanged();
            }
         }
      }
   }
}
