package org.infinispan.distribution.wrappers;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.stats.TransactionsStatisticsRegistry;
import org.infinispan.stats.topK.StreamLibContainer;
import org.infinispan.stats.translations.ExposedStatistics.IspnStats;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;

/**
 * @author Mircea Markus <mircea.markus@jboss.com> (C) 2011 Red Hat Inc.
 * @author Diego Didona <didona@gsd.inesc-id.pt>
 * @author Pedro Ruivo
 * @since 5.2
 */
public class LockManagerWrapper implements LockManager {
   private static final Log log = LogFactory.getLog(LockManagerWrapper.class);

   private final LockManager actual;
   private final StreamLibContainer streamLibContainer;   

   public LockManagerWrapper(LockManager actual, StreamLibContainer streamLibContainer) {
      this.actual = actual;
      this.streamLibContainer = streamLibContainer;
   }

   private boolean updateContentionStats(Object key, TxInvocationContext tctx){
      GlobalTransaction holder = (GlobalTransaction)getOwner(key);
      if(holder!=null){
         GlobalTransaction me = tctx.getGlobalTransaction();
         if(holder!=me){
            if(holder.isRemote()){
               TransactionsStatisticsRegistry.incrementValue(IspnStats.LOCK_CONTENTION_TO_REMOTE);
            } else {
               TransactionsStatisticsRegistry.incrementValue(IspnStats.LOCK_CONTENTION_TO_LOCAL);
            }
            return true;
         }
      }
      return false;
   }

   @Override
   public boolean lockAndRecord(Object key, InvocationContext ctx, long timeoutMillis) throws InterruptedException {
      log.tracef("LockManagerWrapper.lockAndRecord");
      return actual.lockAndRecord(key, ctx, timeoutMillis);
   }

   @Override
   public boolean shareLockAndRecord(Object key, InvocationContext ctx, long timeoutMillis) throws InterruptedException {
      log.tracef("LockManagerWrapper.shareLockAndRecord");
      return actual.shareLockAndRecord(key, ctx, timeoutMillis);
   }

   @Override
   public void unlock(Collection<Object> lockedKeys, Object lockOwner) {
      log.tracef("LockManagerWrapper.unlock");
      actual.unlock(lockedKeys, lockOwner);
   }

   @Override
   public void unlockAll(InvocationContext ctx) {
      log.tracef("LockManagerWrapper.unlockAll");
      actual.unlockAll(ctx);
   }

   @Override
   public boolean ownsLock(Object key, Object owner) {
      log.tracef("LockManagerWrapper.ownsLock");
      return actual.ownsLock(key, owner);
   }

   @Override
   public boolean isLocked(Object key) {
      log.tracef("LockManagerWrapper.isExclusiveLocked");
      return actual.isLocked(key);
   }

   @Override
   public Object getOwner(Object key) {
      log.tracef("LockManagerWrapper.getOwner");
      return actual.getOwner(key);
   }

   @Override
   public String printLockInfo() {
      log.tracef("LockManagerWrapper.printLockInfo");
      return actual.printLockInfo();
   }

   @Override
   public boolean possiblyLocked(CacheEntry entry) {
      log.tracef("LockManagerWrapper.possiblyLocked");
      return actual.possiblyLocked(entry);
   }

   @Override
   public int getNumberOfLocksHeld() {
      log.tracef("LockManagerWrapper.getNumberOfLocksHeld");
      return actual.getNumberOfLocksHeld();
   }

   @Override
   public int getLockId(Object key) {
      log.tracef("LockManagerWrapper.getLockId");
      return actual.getLockId(key);
   }

   @Override
   public boolean acquireLock(InvocationContext ctx, Object key, long timeoutMillis, boolean skipLocking, boolean shared) throws InterruptedException, TimeoutException {
      log.tracef("LockManagerWrapper.acquireLock");

      long lockingTime = 0;
      boolean experiencedContention = false;
      boolean txScope = ctx.isInTxScope();

      if(txScope){
         experiencedContention = this.updateContentionStats(key,(TxInvocationContext)ctx);
         lockingTime = System.nanoTime();
      }
      
      boolean locked;            
      try{
         locked = actual.acquireLock(ctx, key, timeoutMillis, skipLocking, shared);  //this returns false if you already have acquired the lock previously
      } catch(TimeoutException e){
         streamLibContainer.addLockInformation(key, experiencedContention, true);
         throw e;
      } catch(InterruptedException e){
         streamLibContainer.addLockInformation(key, experiencedContention, true);
         throw e;
      }

      streamLibContainer.addLockInformation(key, experiencedContention, false);

      if(txScope && experiencedContention && locked){
         lockingTime = System.nanoTime() - lockingTime;
         TransactionsStatisticsRegistry.addValue(IspnStats.LOCK_WAITING_TIME,lockingTime);
         TransactionsStatisticsRegistry.incrementValue(IspnStats.NUM_WAITED_FOR_LOCKS);
      }
      if(locked){
         TransactionsStatisticsRegistry.addTakenLock(key); //Idempotent
      }


      return locked;
   }

   @Override
   public boolean acquireLockNoCheck(InvocationContext ctx, Object key, long timeoutMillis, boolean skipLocking, boolean shared) throws InterruptedException, TimeoutException {
      log.tracef("LockManagerWrapper.acquireLockNoCheck");

      long lockingTime = 0;
      boolean experiencedContention = false;
      boolean txScope = ctx.isInTxScope();

      if(txScope){
         experiencedContention = this.updateContentionStats(key,(TxInvocationContext)ctx);
         lockingTime = System.nanoTime();
      }

      boolean locked = actual.acquireLockNoCheck(ctx, key, timeoutMillis, skipLocking, shared);

      if(txScope && experiencedContention){
         lockingTime = System.nanoTime() - lockingTime;
         TransactionsStatisticsRegistry.addValue(IspnStats.LOCK_WAITING_TIME,lockingTime);
         TransactionsStatisticsRegistry.incrementValue(IspnStats.NUM_WAITED_FOR_LOCKS);
      }
      if(locked){
         TransactionsStatisticsRegistry.addTakenLock(key); //Idempotent
      }
      return locked;
   }   
}
