package org.infinispan.util.concurrent.locks;

import org.infinispan.util.concurrent.TimeoutException;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public interface LockPromise {

   LockPromise NO_OP = new LockPromise() {
      @Override
      public boolean isAvailable() {
         return true;
      }

      @Override
      public void lock() throws InterruptedException, TimeoutException {/*no-op*/}

      @Override
      public void addListener(Listener listener) {
         listener.onEvent(LockState.AVAILABLE);
      }

      @Override
      public void setDeadlockChecker(DeadlockChecker deadlockChecker) {/*no-op*/}
   };

   /**
    * It tests if the lock is available.
    * <p>
    * The lock is consider available when it is successfully acquired or the timeout is expired. In any case, when it
    * returns {@code true}, the {@link #lock()} will never block.
    *
    * @return {@code true} if the lock is available (or the timeout is expired), {@code false} otherwise.
    */
   boolean isAvailable();

   /**
    * It locks the key (or keys) associated to this promise.
    * <p>
    * This method will block until the lock is available or the timeout is expired.
    *
    * @throws InterruptedException if the current thread is interrupted while acquiring the lock
    * @throws TimeoutException     if we are unable to acquire the lock after a specified timeout.
    */
   void lock() throws InterruptedException, TimeoutException;

   /**
    * Adds a {@link org.infinispan.util.concurrent.locks.LockPromise.Listener} to be invoked when the lock is
    * available.
    * <p>
    * The {@code acquired} parameter indicates that the lock is acquired (when it is {@code true}) or it timed out (when
    * it is {@code false}).
    *
    * @param listener the {@link org.infinispan.util.concurrent.locks.LockPromise.Listener} to invoke.
    */
   void addListener(Listener listener);

   void setDeadlockChecker(DeadlockChecker deadlockChecker);

   interface Listener {

      /**
       * Invoked when the lock is available.
       *
       * @param state the current lock state. It can be {@link LockState#AVAILABLE}, {@link LockState#TIMED_OUT} or
       *              {@link LockState#DEADLOCKED}.
       */
      void onEvent(LockState state);
   }

}
