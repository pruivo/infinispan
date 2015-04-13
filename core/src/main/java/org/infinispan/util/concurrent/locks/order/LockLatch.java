package org.infinispan.util.concurrent.locks.order;

/**
 * It represents an intent to lock a key.
 * <p/>
 * It is used to manage the remote commands received by a node and it will change it state when the chances of lock
 * contention are minimal. Note that it never ensures no contention on the lock because the lock may be acquired by
 * concurrent local commands.
 * <p/>
 * Each latch represents a remote command and can guard multiple locks.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public interface LockLatch {

   public static LockLatch NO_OP = new LockLatch() {
      @Override
      public boolean isReady() {
         return true;
      }

      @Override
      public void release() {
         //no-op
      }
   };

   /**
    * @return {@code true} when it the chance of lock contention is minimal.
    */
   boolean isReady();

   /**
    * It releases the {@link org.infinispan.util.concurrent.locks.order.LockLatch} unblocking pending commands.
    */
   void release();
}
