package org.infinispan.util.concurrent.locks.containers.readwrite;

import org.infinispan.util.concurrent.locks.OwnableRefCountingReentrantReadWriteLock;
import org.infinispan.util.concurrent.locks.containers.AbstractPerEntryLockContainer;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.concurrent.TimeUnit;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class OwnableReentrantPerEntryReadWriteLockContainer extends AbstractPerEntryLockContainer<OwnableRefCountingReentrantReadWriteLock> {

   private static final Log log = LogFactory.getLog(OwnableReentrantPerEntryReadWriteLockContainer.class);

   public OwnableReentrantPerEntryReadWriteLockContainer(int concurrencyLevel) {
      super(concurrencyLevel);
   }

   @Override
   protected OwnableRefCountingReentrantReadWriteLock newLock() {
      return new OwnableRefCountingReentrantReadWriteLock();
   }

   @Override
   protected void unlock(OwnableRefCountingReentrantReadWriteLock toRelease, Object ctx) {
      toRelease.unlock(ctx);
   }

   @Override
   protected boolean tryExclusiveLock(OwnableRefCountingReentrantReadWriteLock lock, long timeout, TimeUnit unit, Object lockOwner) throws InterruptedException {
      return lock.tryLock(lockOwner, timeout, unit);
   }

   @Override
   protected boolean tryShareLock(OwnableRefCountingReentrantReadWriteLock lock, long timeout, TimeUnit unit, Object lockOwner) throws InterruptedException {
      return lock.tryShareLock(lockOwner, timeout, unit);
   }

   @Override
   protected void exclusiveLock(OwnableRefCountingReentrantReadWriteLock lock, Object lockOwner) {
      lock.lock(lockOwner);
   }

   @Override
   protected void shareLock(OwnableRefCountingReentrantReadWriteLock lock, Object lockOwner) {
      lock.lockShare(lockOwner);
   }

   @Override
   public boolean ownsExclusiveLock(Object key, Object owner) {
      OwnableRefCountingReentrantReadWriteLock l = getLockFromMap(key);
      return l != null && owner.equals(l.getOwner()); 
   }

   @Override
   public boolean isExclusiveLocked(Object key) {
      OwnableRefCountingReentrantReadWriteLock l = getLockFromMap(key);
      return l != null && l.isLocked();
   }

   @Override
   public boolean ownsShareLock(Object key, Object owner) {
      OwnableRefCountingReentrantReadWriteLock l = getLockFromMap(key);
      return l != null && l.ownsShareLock(owner);
   }

   @Override
   public boolean isSharedLocked(Object key) {
      OwnableRefCountingReentrantReadWriteLock l = getLockFromMap(key);
      return l != null && l.isShareLocked();
   }

   @Override
   protected Log getLog() {
      return log;
   }

   private OwnableRefCountingReentrantReadWriteLock getLockFromMap(Object key) {
      return locks.get(key);
   }
}
