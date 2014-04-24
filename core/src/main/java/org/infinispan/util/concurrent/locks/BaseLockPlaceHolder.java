package org.infinispan.util.concurrent.locks;

import org.infinispan.util.concurrent.TimeoutException;

import java.util.concurrent.locks.AbstractQueuedSynchronizer;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
public abstract class BaseLockPlaceHolder implements LockPlaceHolder {

   private static final int QUEUE = 0;
   private static final int READY = 1;
   private static final int TIMEOUT = 2;

   private final Sync sync;

   protected BaseLockPlaceHolder() {
      sync = new Sync();
   }

   @Override
   public final boolean isReady() {
      checkStatus();
      return sync.getCurrentState() != QUEUE;
   }

   @Override
   public final void awaitReady() throws InterruptedException {
      sync.acquireShared(0);
      if (sync.getCurrentState() == TIMEOUT) {
         throw new TimeoutException("Lock Timeout!");
      }
   }

   protected abstract boolean isTimedOut();

   protected abstract void onTimeout();

   protected abstract boolean checkReady();

   protected final void checkStatus() {
      if (sync.getCurrentState() == QUEUE) {
         if (checkReady()) {
            sync.releaseShared(READY);
         } else if (isTimedOut()) {
            sync.releaseShared(TIMEOUT);
         }
      }
   }

   private class Sync extends AbstractQueuedSynchronizer {

      private Sync() {
         super();
         setState(QUEUE);
      }

      public int getCurrentState() {
         return getState();
      }

      @Override
      protected int tryAcquireShared(int ignored) {
         return getState() == QUEUE ? -1 : 1;
      }

      @Override
      protected boolean tryReleaseShared(int arg) {
         if (compareAndSetState(QUEUE, arg) && arg == TIMEOUT) {
            onTimeout();
         }
         return true;
      }
   }
}

