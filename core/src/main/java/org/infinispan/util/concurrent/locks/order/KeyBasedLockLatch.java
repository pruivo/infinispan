package org.infinispan.util.concurrent.locks.order;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class KeyBasedLockLatch implements ExtendedLockLatch {
   private final Object key;
   private final BaseRemoteLockOrderManager<KeyBasedLockLatch> remoteLockOrderManager;
   private volatile boolean ready;

   public KeyBasedLockLatch(Object key, BaseRemoteLockOrderManager<KeyBasedLockLatch> remoteLockOrderManager) {
      this.key = key;
      this.remoteLockOrderManager = remoteLockOrderManager;
      this.ready = false;
   }

   public Object getKey() {
      return key;
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
      return "KeyBasedLockLatch{" +
            "key=" + key +
            ", ready=" + ready +
            ", id=" + hashCode() +
            '}';
   }
}
