package org.infinispan.util.concurrent.locks.impl;

import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.commons.util.concurrent.jdk8backported.EquivalentConcurrentHashMapV8;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.util.TimeService;
import org.infinispan.util.concurrent.locks.LockContainerV8;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public class PerKeyLockContainer implements LockContainerV8 {

   private final EquivalentConcurrentHashMapV8<Object, LockWrapper> lockMap;
   private TimeService timeService;

   public PerKeyLockContainer(Equivalence<Object> keyEquivalence) {
      lockMap = new EquivalentConcurrentHashMapV8<>(keyEquivalence, AnyEquivalence.getInstance());
   }

   @Inject
   public void inject(TimeService timeService) {
      this.timeService = timeService;
   }

   @Override
   public InfinispanLock get(Object key) {
      return lockMap.compute(key, new EquivalentConcurrentHashMapV8.BiFun<Object, LockWrapper, LockWrapper>() {
         @Override
         public LockWrapper apply(Object o, LockWrapper lockWrapper) {
            LockWrapper retVal = lockWrapper;
            if (retVal == null) {
               retVal = new LockWrapper(PerKeyLockContainer.this.createInfinispanLock(o));
            }
            retVal.increment();
            return retVal;
         }
      }).getLock();
   }

   @Override
   public InfinispanLock peek(Object key) {
      LockWrapper wrapper = lockMap.get(key);
      return wrapper == null ? null : wrapper.getLock();
   }

   @Override
   public int getNumLocksHeld() {
      int count = 0;
      for (LockWrapper wrapper : lockMap.values()) {
         if (!wrapper.getLock().isFree()) {
            count++;
         }
      }
      return count;
   }

   @Override
   public int size() {
      return lockMap.size();
   }

   private InfinispanLock createInfinispanLock(Object key) {
      return new InfinispanLock(timeService, new Runnable() {
         @Override
         public void run() {
            lockMap.compute(key, new EquivalentConcurrentHashMapV8.BiFun<Object, LockWrapper, LockWrapper>() {
               @Override
               public LockWrapper apply(Object o, LockWrapper lockWrapper) {
                  if (lockWrapper != null) {
                     lockWrapper.decrement();
                     if (lockWrapper.get() == 0) {
                        return null;
                     }
                  }
                  return lockWrapper;
               }
            });
         }
      });
   }

   private static class LockWrapper {
      private final InfinispanLock lock;
      private int refCount;

      private LockWrapper(InfinispanLock lock) {
         this.lock = lock;
      }

      public InfinispanLock getLock() {
         return lock;
      }

      public void increment() {
         refCount++;
      }

      public void decrement() {
         refCount--;
      }

      public int get() {
         return refCount;
      }
   }
}
