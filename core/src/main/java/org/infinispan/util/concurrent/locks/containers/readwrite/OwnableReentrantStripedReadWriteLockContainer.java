package org.infinispan.util.concurrent.locks.containers.readwrite;

import org.infinispan.util.concurrent.locks.OwnableReentrantReadWriteLock;
import org.infinispan.util.concurrent.locks.containers.AbstractStripedLockContainer;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.concurrent.TimeUnit;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class OwnableReentrantStripedReadWriteLockContainer extends AbstractStripedLockContainer<OwnableReentrantReadWriteLock> {

   private static final Log log = LogFactory.getLog(OwnableReentrantStripedReadWriteLockContainer.class);
   
   private OwnableReentrantReadWriteLock[] sharedLocks;

   public OwnableReentrantStripedReadWriteLockContainer(int concurrencyLevel) {
      initLocks(calculateNumberOfSegments(concurrencyLevel));
   }

   @Override
   protected void initLocks(int numLocks) {
      sharedLocks = new OwnableReentrantReadWriteLock[numLocks];
      for (int i = 0; i < numLocks; i++) sharedLocks[i] = new OwnableReentrantReadWriteLock();
   }

   @Override
   protected void unlock(OwnableReentrantReadWriteLock toRelease, Object owner) {
      toRelease.unlock(owner);
   }

   @Override
   protected boolean tryExclusiveLock(OwnableReentrantReadWriteLock lock, long timeout, TimeUnit unit, Object lockOwner) throws InterruptedException {
      return lock.tryLock(lockOwner, timeout, unit);
   }

   @Override
   protected boolean tryShareLock(OwnableReentrantReadWriteLock lock, long timeout, TimeUnit unit, Object lockOwner) throws InterruptedException {
      return lock.tryShareLock(lockOwner, timeout, unit);
   }

   @Override
   protected void exclusiveLock(OwnableReentrantReadWriteLock lock, Object lockOwner) {
      lock.lock(lockOwner);
   }

   @Override
   protected void shareLock(OwnableReentrantReadWriteLock lock, Object lockOwner) {
      lock.lockShare(lockOwner);
   }

   @Override
   protected Log getLog() {
      return log;
   }

   @Override
   public boolean ownsExclusiveLock(Object key, Object owner) {
      OwnableReentrantReadWriteLock lock = getLock(key);
      return owner.equals(lock.getOwner());
   }

   @Override
   public boolean ownsShareLock(Object key, Object owner) {
      return getLock(key).ownsShareLock(owner);
   }

   @Override
   public boolean isExclusiveLocked(Object key) {
      return getLock(key).isLocked();
   }

   @Override
   public boolean isSharedLocked(Object key) {
      return getLock(key).isShareLocked();
   }

   @Override
   public OwnableReentrantReadWriteLock getLock(Object key) {
      return sharedLocks[hashToIndex(key)];
   }

   @Override
   public int getNumLocksHeld() {
      int i = 0;
      for (OwnableReentrantReadWriteLock l : sharedLocks) if (l.isLocked()) i++;
      return i;
   }

   @Override
   public int size() {
      return sharedLocks.length;
   }
}
