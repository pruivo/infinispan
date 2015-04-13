package org.infinispan.util.concurrent.locks.order;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class IndexBasedLockLatch implements ExtendedLockLatch {
   private final int index;
   private final BaseRemoteLockOrderManager<IndexBasedLockLatch> remoteLockOrderManager;
   private volatile boolean ready;

   public IndexBasedLockLatch(int index, BaseRemoteLockOrderManager<IndexBasedLockLatch> remoteLockOrderManager) {
      this.index = index;
      this.remoteLockOrderManager = remoteLockOrderManager;
      this.ready = false;
   }

   public int getIndex() {
      return index;
   }

   @Override
   public void markReady() {
      this.ready = true;
   }

   @Override
   public boolean isReady() {
      return ready;
   }

   @Override
   public void release() {
      remoteLockOrderManager.release(this);
   }

   @Override
   public String toString() {
      return "HashBasedLockLatch{" +
            "index=" + index +
            ", ready=" + ready +
            ", id=" + hashCode() +
            '}';
   }
}
