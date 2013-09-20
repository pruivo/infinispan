package org.infinispan.stats.wrappers;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.stats.container.LockStatisticsContainer;
import org.infinispan.stats.manager.StatisticsManager;
import org.infinispan.util.TimeService;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.locks.DeadlockDetectedException;
import org.infinispan.util.concurrent.locks.LockManager;

import java.util.Collection;

/**
 * Takes statistic about lock acquisition.
 *
 * @author Roberto Palmieri
 * @author Sebastiano Peluso
 * @author Diego Didona
 * @author Pedro Ruivo
 * @since 6.0
 */
public class ExtendedStatisticLockManagerv2 implements LockManager {
   private final LockManager actual;
   private final StatisticsManager statisticsManager;
   private final TimeService timeService;

   public ExtendedStatisticLockManagerv2(LockManager actual, StatisticsManager statisticsManager,
                                         TimeService timeService) {
      this.statisticsManager = statisticsManager;
      this.actual = actual;
      this.timeService = timeService;
   }

   public final LockManager getActual() {
      return actual;
   }

   @SuppressWarnings("ConstantConditions")
   @Override
   public boolean lockAndRecord(Object key, InvocationContext ctx, long timeoutMillis) throws InterruptedException {
      LockStatisticsContainer container = statisticsManager.getLockStatisticsContainer(ctx.getLockOwner());
      if (container == null) {
         return actual.lockAndRecord(key, ctx, timeoutMillis);
      }
      final boolean contended = isContented(key, ctx.getLockOwner());
      final long start = contended ? timeService.time() : 0;
      boolean locked = false;
      boolean timeout = false;
      boolean deadlock = false;
      try {
         //this returns false if you already have acquired the lock previously
         locked = actual.lockAndRecord(key, ctx, timeoutMillis);
      } catch (TimeoutException e) {
         timeout = true;
         throw e;
      } catch (DeadlockDetectedException e) {
         deadlock = true;
         throw e;
      } finally {
         final long end = contended ? timeService.time() : 0;
         if (locked) {
            container.keyLocked(key, end - start);
         } else if (timeout) {
            container.lockTimeout(end - start);
         } else if (deadlock) {
            container.deadlock(end - start);
         }
      }

      return locked;
   }

   @Override
   public void unlock(Collection<Object> lockedKeys, Object lockOwner) {
      LockStatisticsContainer container = statisticsManager.getLockStatisticsContainer(lockOwner);
      if (container == null) {
         actual.unlock(lockedKeys, lockOwner);
         return;
      }
      try {
         actual.unlock(lockedKeys, lockOwner);
      } finally {
         container.keysUnlocked(lockedKeys);
      }
   }

   @Override
   public void unlockAll(InvocationContext ctx) {
      LockStatisticsContainer container = statisticsManager.getLockStatisticsContainer(ctx.getLockOwner());
      if (container == null) {
         actual.unlockAll(ctx);
         return;
      }
      Collection<Object> lockedKeys = ctx.getLockedKeys();
      try {
         actual.unlockAll(ctx);
      } finally {
         container.keysUnlocked(lockedKeys);
      }
   }

   @Override
   public boolean ownsLock(Object key, Object owner) {
      return actual.ownsLock(key, owner);
   }

   @Override
   public boolean isLocked(Object key) {
      return actual.isLocked(key);
   }

   @Override
   public Object getOwner(Object key) {
      return actual.getOwner(key);
   }

   @Override
   public String printLockInfo() {
      return actual.printLockInfo();
   }

   @Override
   public boolean possiblyLocked(CacheEntry entry) {
      return actual.possiblyLocked(entry);
   }

   @Override
   public int getNumberOfLocksHeld() {
      return actual.getNumberOfLocksHeld();
   }

   @Override
   public int getLockId(Object key) {
      return actual.getLockId(key);
   }

   @SuppressWarnings("ConstantConditions")
   @Override
   public boolean acquireLock(InvocationContext ctx, Object key, long timeoutMillis, boolean skipLocking) throws InterruptedException, TimeoutException {
      LockStatisticsContainer container = statisticsManager.getLockStatisticsContainer(ctx.getLockOwner());
      if (container == null) {
         return actual.acquireLock(ctx, key, timeoutMillis, skipLocking);
      }
      final boolean contended = isContented(key, ctx.getLockOwner());
      final long start = contended ? timeService.time() : 0;
      boolean locked = false;
      boolean timeout = false;
      boolean deadlock = false;
      try {
         //this returns false if you already have acquired the lock previously
         locked = actual.acquireLock(ctx, key, timeoutMillis, skipLocking);
      } catch (TimeoutException e) {
         timeout = true;
         throw e;
      } catch (DeadlockDetectedException e) {
         deadlock = true;
         throw e;
      } finally {
         final long end = contended ? timeService.time() : 0;
         if (locked) {
            container.keyLocked(key, end - start);
         } else if (timeout) {
            container.lockTimeout(end - start);
         } else if (deadlock) {
            container.deadlock(end - start);
         }
      }

      return locked;
   }

   @SuppressWarnings("ConstantConditions")
   @Override
   public boolean acquireLockNoCheck(InvocationContext ctx, Object key, long timeoutMillis, boolean skipLocking) throws InterruptedException, TimeoutException {
      LockStatisticsContainer container = statisticsManager.getLockStatisticsContainer(ctx.getLockOwner());
      if (container == null) {
         return actual.acquireLockNoCheck(ctx, key, timeoutMillis, skipLocking);
      }
      final boolean contended = isContented(key, ctx.getLockOwner());
      final long start = contended ? timeService.time() : 0;
      boolean locked = false;
      boolean timeout = false;
      boolean deadlock = false;
      try {
         //this returns false if you already have acquired the lock previously
         locked = actual.acquireLockNoCheck(ctx, key, timeoutMillis, skipLocking);
      } catch (TimeoutException e) {
         timeout = true;
         throw e;
      } catch (DeadlockDetectedException e) {
         deadlock = true;
         throw e;
      } finally {
         final long end = contended ? timeService.time() : 0;
         if (locked) {
            container.keyLocked(key, end - start);
         } else if (timeout) {
            container.lockTimeout(end - start);
         } else if (deadlock) {
            container.deadlock(end - start);
         }
      }

      return locked;
   }

   private boolean isContented(Object key, Object lockOwner) {
      Object holder = getOwner(key);
      return holder != null && !lockOwner.equals(holder);
   }

}
