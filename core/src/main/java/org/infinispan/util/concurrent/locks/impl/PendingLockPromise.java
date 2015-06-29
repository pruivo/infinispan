package org.infinispan.util.concurrent.locks.impl;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public interface PendingLockPromise {

   PendingLockPromise NO_OP = new PendingLockPromise() {
      @Override
      public boolean isReady() {
         return true;
      }

      @Override
      public void addListener(Listener listener) {
         listener.onReady();
      }

      @Override
      public boolean hasTimedOut() {
         return false;
      }

      @Override
      public long getRemainingTimeout() {
         return Long.MAX_VALUE;
      }


   };

   boolean isReady();

   void addListener(Listener listener);

   boolean hasTimedOut();

   long getRemainingTimeout();

   interface Listener {
      void onReady();
   }

}
