package org.infinispan.util.concurrent.locks;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public final class NoOpLockPlaceHolder implements LockPlaceHolder {

   public static final LockPlaceHolder INSTANCE = new NoOpLockPlaceHolder();

   private NoOpLockPlaceHolder() {
   }

   @Override
   public boolean isReady() {
      return true;
   }

   @Override
   public void awaitReady() throws InterruptedException {
      //no-op
   }
}
