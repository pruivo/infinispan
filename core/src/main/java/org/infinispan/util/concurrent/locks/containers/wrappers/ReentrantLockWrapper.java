package org.infinispan.util.concurrent.locks.containers.wrappers;

import org.infinispan.util.concurrent.locks.VisibleOwnerReentrantLock;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
public class ReentrantLockWrapper extends BaseLockWrapper implements LockWrapper {

   private static final String ANOTHER_THREAD = "(another thread)";
   private final VisibleOwnerReentrantLock lock;

   public ReentrantLockWrapper() {
      this.lock = new VisibleOwnerReentrantLock();
   }

   @Override
   public Object getOwner() {
      Object owner = lock.getOwner();
      return owner == null ? ANOTHER_THREAD : owner;
   }

   @Override
   public boolean isLocked() {
      return lock.isLocked();
   }

   @Override
   public boolean tryLock(Object lockOwner, long timeout, TimeUnit unit) throws InterruptedException {
      return lock.tryLock(timeout, unit);
   }

   @Override
   public void lock(Object lockOwner) {
      lock.lock();
   }

   @Override
   protected boolean internalUnlock(Object owner) {
      lock.unlock();
      return !lock.isLocked();
   }
}
