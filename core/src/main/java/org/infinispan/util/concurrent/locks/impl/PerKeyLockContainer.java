package org.infinispan.util.concurrent.locks.impl;

import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.commons.util.concurrent.jdk8backported.EquivalentConcurrentHashMapV8;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.util.TimeService;
import org.infinispan.util.concurrent.locks.CancellableLockPromise;
import org.infinispan.util.concurrent.locks.LockContainerV8;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public class PerKeyLockContainer implements LockContainerV8 {

   private final EquivalentConcurrentHashMapV8<Object, InfinispanLock> lockMap;
   private TimeService timeService;

   public PerKeyLockContainer(Equivalence<Object> keyEquivalence) {
      lockMap = new EquivalentConcurrentHashMapV8<>(keyEquivalence, AnyEquivalence.getInstance());
   }

   @Inject
   public void inject(TimeService timeService) {
      this.timeService = timeService;
   }

   @Override
   public CancellableLockPromise acquire(Object key, Object lockOwner, long time, TimeUnit timeUnit) {
      AtomicReference<CancellableLockPromise> reference = new AtomicReference<>();
      lockMap.compute(key, new BiFunction<Object, InfinispanLock, InfinispanLock>() {
         @Override
         public InfinispanLock apply(Object key, InfinispanLock lock) {
            if (lock == null) {
               lock = createInfinispanLock(key);
            }
            reference.set(lock.acquire(lockOwner, time, timeUnit));
            return lock;
         }
      });
      return reference.get();
   }

   @Override
   public InfinispanLock getLock(Object key) {
      return lockMap.get(key);
   }

   @Override
   public void release(Object key, Object lockOwner) {
      lockMap.computeIfPresent(key, new BiFunction<Object, InfinispanLock, InfinispanLock>() {
         @Override
         public InfinispanLock apply(Object key, InfinispanLock lock) {
            lock.release(lockOwner);
            return lock.isEmpty() ? null : lock; //remove it if empty
         }
      });
   }

   @Override
   public int getNumLocksHeld() {
      int count = 0;
      for (InfinispanLock lock : lockMap.values()) {
         if (!lock.isFree()) {
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
            lockMap.computeIfPresent(key, new BiFunction<Object, InfinispanLock, InfinispanLock>() {
               @Override
               public InfinispanLock apply(Object key, InfinispanLock lock) {
                  return lock.isFree() ? null : lock;
               }
            });
         }
      });
   }

}
