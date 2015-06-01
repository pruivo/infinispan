package org.infinispan.stats.wrappers;

import org.infinispan.context.InvocationContext;
import org.infinispan.stats.topK.StreamSummaryContainer;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.concurrent.locks.LockPromise;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * Top-key stats about locks.
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
public class TopKeyLockManager implements LockManager {

   private final LockManager current;
   private final StreamSummaryContainer container;

   public TopKeyLockManager(LockManager current, StreamSummaryContainer container) {
      this.current = current;
      this.container = container;
   }

   @Override
   public LockPromise lock(Object key, Object lockOwner, long time, TimeUnit unit) {
      LockPromise lockPromise = current.lock(key, lockOwner, time, unit);
      final boolean contented = !lockOwner.equals(current.getOwner(key));
      lockPromise.addListener(acquired -> container.addLockInformation(key, contented, !acquired));
      return lockPromise;
   }

   @Override
   public LockPromise lockAll(Collection<?> keys, Object lockOwner, long time, TimeUnit unit) {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public void unlock(Object key, Object lockOwner) {
      current.unlock(key, lockOwner);
   }

   @Override
   public void unlockAll(Collection<?> keys, Object lockOwner) {
      current.unlockAll(keys, lockOwner);
   }

   @Override
   public void unlockAll(InvocationContext ctx) {
      current.unlockAll(ctx);
   }

   @Override
   public boolean ownsLock(Object key, Object owner) {
      return current.ownsLock(key, owner);
   }

   @Override
   public boolean isLocked(Object key) {
      return current.isLocked(key);
   }

   @Override
   public Object getOwner(Object key) {
      return current.getOwner(key);
   }

   @Override
   public String printLockInfo() {
      return current.printLockInfo();
   }

   @Override
   public int getNumberOfLocksHeld() {
      return current.getNumberOfLocksHeld();
   }

   @Override
   public int getConcurrencyLevel() {
      return current.getConcurrencyLevel();
   }

   @Override
   public int getNumberOfLocksAvailable() {
      return current.getNumberOfLocksAvailable();
   }

   @Override
   public long getDefaultTimeoutMillis() {
      return current.getDefaultTimeoutMillis();
   }
}
