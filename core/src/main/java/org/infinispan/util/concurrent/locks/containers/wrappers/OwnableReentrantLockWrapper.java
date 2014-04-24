package org.infinispan.util.concurrent.locks.containers.wrappers;

import org.infinispan.util.concurrent.locks.OwnableReentrantLock;

import java.util.concurrent.TimeUnit;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
public class OwnableReentrantLockWrapper extends BaseLockWrapper implements LockWrapper {

   private final OwnableReentrantLock lock;

   public OwnableReentrantLockWrapper() {
      lock = new OwnableReentrantLock();
   }

   @Override
   public Object getOwner() {
      return lock.getOwner();
   }

   @Override
   public boolean isLocked() {
      return lock.isLocked();
   }

   @Override
   public boolean tryLock(Object lockOwner, long timeout, TimeUnit unit) throws InterruptedException {
      return lock.tryLock(lockOwner, timeout, unit);
   }

   @Override
   public void lock(Object lockOwner) {
      lock.lock(lockOwner);
   }

   @Override
   protected boolean internalUnlock(Object owner) {
      return lock.unlock(owner);
   }
}
