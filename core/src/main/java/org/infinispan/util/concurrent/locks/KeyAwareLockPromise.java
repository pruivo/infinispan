package org.infinispan.util.concurrent.locks;

import org.infinispan.util.concurrent.TimeoutException;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public interface KeyAwareLockPromise extends LockPromise {

   KeyAwareLockPromise NO_OP = new KeyAwareLockPromise() {
      @Override
      public void addListener(KeyAwareLockListener listener) {/*no-op*/} //there is no key!

      public boolean isAvailable() {
         return LockPromise.NO_OP.isAvailable();
      }

      public void lock() throws InterruptedException, TimeoutException {
         LockPromise.NO_OP.lock();
      }

      public void addListener(LockListener listener) {
         LockPromise.NO_OP.addListener(listener);
      }

      public void setDeadlockChecker(DeadlockChecker deadlockChecker) {
         LockPromise.NO_OP.setDeadlockChecker(deadlockChecker);
      }
   };

   void addListener(KeyAwareLockListener listener);

}
