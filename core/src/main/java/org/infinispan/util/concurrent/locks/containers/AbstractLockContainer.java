package org.infinispan.util.concurrent.locks.containers;

import org.infinispan.factories.annotations.Inject;
import org.infinispan.util.TimeService;
import org.infinispan.util.concurrent.locks.LockPlaceHolder;
import org.infinispan.util.concurrent.locks.NoOpLockPlaceHolder;
import org.infinispan.util.concurrent.locks.TimeServiceLockPlaceHolder;
import org.infinispan.util.concurrent.locks.containers.wrappers.LockHolder;
import org.infinispan.util.concurrent.locks.containers.wrappers.LockWrapper;
import org.infinispan.util.logging.Log;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public abstract class AbstractLockContainer<L extends LockWrapper> implements LockContainer {

   private final Object preAcquireLock = new Object();
   private TimeService timeService;

   @Override
   public final LockPlaceHolder preAcquireLocks(Object lockOwner, long timeout, TimeUnit timeUnit, Object... keys) {
      if (keys.length == 0) {
         return NoOpLockPlaceHolder.INSTANCE;
      }
      Set<Object> unique = new HashSet<Object>(Arrays.asList(keys));
      final ArrayList<LockHolder> lockHolders = new ArrayList<LockHolder>(unique.size());
      synchronized (preAcquireLock) {
         for (Object key : unique) {
            lockHolders.add(addLockHolder(key, lockOwner));
         }
      }
      LockPlaceHolderImpl placeHolder = new LockPlaceHolderImpl(timeService, timeout, timeUnit, lockHolders);
      return placeHolder.registerListener();
   }

   @Inject
   public void injectTimeService(TimeService timeService) {
      this.timeService = timeService;
   }

   @Override
   public final boolean ownsLock(Object key, Object owner) {
      L l = getLock(key);
      return l != null && owner.equals(l.getOwner());
   }

   @Override
   public final boolean isLocked(Object key) {
      L l = getLock(key);
      return l != null && l.isLocked();
   }

   @Override
   public final Object getLockOwner(Object key) {
      L l = getLock(key);
      return l != null ? l.getOwner() : null;
   }

   public abstract boolean isQueuesEmpty();

   protected abstract LockHolder addLockHolder(Object key, Object lockOwner);

   /**
    * Releases a lock and swallows any IllegalMonitorStateExceptions - so it is safe to call this method even if the
    * lock is not locked, or not locked by the current thread.
    *
    * @param toRelease lock to release
    */
   protected void safeRelease(L toRelease, Object lockOwner) {
      if (toRelease != null) {
         try {
            toRelease.unlock(lockOwner);
         } catch (IllegalMonitorStateException imse) {
            // Perhaps the caller hadn't acquired the lock after all.
         }
      }
   }

   protected abstract L getLock(Object key);

   protected abstract Log getLog();

   protected abstract void lockHolderTimeout(LockHolder lockHolder);

   private void lockPlaceHolderTimeout(Collection<LockHolder> lockPlaceHolderCollection) {
      for (LockHolder lockHolder : lockPlaceHolderCollection) {
         lockHolderTimeout(lockHolder);
      }
   }

   private class LockPlaceHolderImpl extends TimeServiceLockPlaceHolder implements LockHolder.LockHolderListener,
                                                                                   LockPlaceHolder {

      private final Collection<LockHolder> lockHolders;

      private LockPlaceHolderImpl(TimeService timeService, long timeout, TimeUnit timeUnit,
                                  Collection<LockHolder> lockHolders) {
         super(timeService, timeout, timeUnit);
         this.lockHolders = Collections.unmodifiableCollection(lockHolders);
      }

      public LockPlaceHolderImpl registerListener() {
         for (LockHolder lockHolder : lockHolders) {
            lockHolder.addListener(this);
         }
         return this;
      }

      @Override
      public void notifyAcquire() {
         checkStatus();
      }

      @Override
      protected void onTimeout() {
         lockPlaceHolderTimeout(lockHolders);
      }

      @Override
      protected boolean checkReady() {
         for (LockHolder lockPlaceHolder : lockHolders) {
            if (!lockPlaceHolder.isReady()) {
               return false;
            }
         }
         return true;
      }
   }
}
