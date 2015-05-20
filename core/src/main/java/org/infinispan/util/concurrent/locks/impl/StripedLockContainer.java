package org.infinispan.util.concurrent.locks.impl;

import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.util.StripedHashFunction;
import org.infinispan.util.TimeService;
import org.infinispan.util.concurrent.locks.LockContainerV8;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public class StripedLockContainer implements LockContainerV8 {

   private final InfinispanLock[] sharedLocks;
   private final StripedHashFunction<Object> hashFunction;

   public StripedLockContainer(int concurrencyLevel, Equivalence<Object> keyEquivalence, TimeService timeService) {
      this.hashFunction = new StripedHashFunction<>(keyEquivalence, concurrencyLevel);
      int numLocks = hashFunction.getNumSegments();
      sharedLocks = new InfinispanLock[numLocks];
      for (int i = 0; i < numLocks; i++) sharedLocks[i] = new InfinispanLock(timeService);
   }

   @Override
   public InfinispanLock get(Object key) {
      return peek(key);
   }

   @Override
   public InfinispanLock peek(Object key) {
      return sharedLocks[hashFunction.hashToSegment(key)];
   }

   @Override
   public int getNumLocksHeld() {
      int count = 0;
      for (InfinispanLock lock : sharedLocks) {
         if (!lock.isFree()) {
            count++;
         }
      }
      return count;
   }

   @Override
   public int size() {
      return sharedLocks.length;
   }
}
