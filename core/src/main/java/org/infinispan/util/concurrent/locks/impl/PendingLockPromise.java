package org.infinispan.util.concurrent.locks.impl;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public interface PendingLockPromise {

   public static PendingLockPromise NO_OP = new PendingLockPromise() {
      @Override
      public boolean isReady() {
         return true;
      }

      @Override
      public void addListener(Listener listener) {
         listener.onReady();
      }
   };

   boolean isReady();

   void addListener(Listener listener);

   interface Listener {
      void onReady();
   }

}
