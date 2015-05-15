package org.infinispan.lock;

import org.infinispan.test.AbstractCacheTest;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.locks.LockPromise;
import org.infinispan.util.concurrent.locks.impl.InfinispanLock;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Some unit tests for the InfinispanLock
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
@Test(groups = "unit", testName = "lock.InfinispanLockTest")
public class InfinispanLockTest {

   public void testTimeout() throws InterruptedException {
      final String lockOwner1 = "LO1";
      final String lockOwner2 = "LO2";

      final InfinispanLock lock = new InfinispanLock(AbstractCacheTest.TIME_SERVICE);
      final LockPromise lockPromise1 = lock.acquire(lockOwner1, 0, TimeUnit.MILLISECONDS);
      final LockPromise lockPromise2 = lock.acquire(lockOwner2, 0, TimeUnit.MILLISECONDS);

      AssertJUnit.assertTrue(lockPromise1.isAvailable());
      AssertJUnit.assertTrue(lockPromise2.isAvailable());

      lockPromise1.lock();
      AssertJUnit.assertEquals(lockOwner1, lock.getLockOwner());
      try {
         lockPromise2.lock();
         AssertJUnit.fail();
      } catch (TimeoutException e) {
         //expected!
      }
      lock.release(lockOwner1);
      AssertJUnit.assertNull(lock.getLockOwner());
      AssertJUnit.assertTrue(lock.isFree());

      //no side effects
      lock.release(lockOwner2);
      AssertJUnit.assertTrue(lock.isFree());
      AssertJUnit.assertNull(lock.getLockOwner());
   }

   public void testTimeout2() throws InterruptedException {
      final String lockOwner1 = "LO1";
      final String lockOwner2 = "LO2";
      final String lockOwner3 = "LO3";

      final InfinispanLock lock = new InfinispanLock(AbstractCacheTest.TIME_SERVICE);
      final LockPromise lockPromise1 = lock.acquire(lockOwner1, 0, TimeUnit.MILLISECONDS);
      final LockPromise lockPromise2 = lock.acquire(lockOwner2, 0, TimeUnit.MILLISECONDS);
      final LockPromise lockPromise3 = lock.acquire(lockOwner3, 1, TimeUnit.DAYS);

      AssertJUnit.assertTrue(lockPromise1.isAvailable());
      AssertJUnit.assertTrue(lockPromise2.isAvailable());
      AssertJUnit.assertFalse(lockPromise3.isAvailable());

      lockPromise1.lock();
      AssertJUnit.assertEquals(lockOwner1, lock.getLockOwner());
      try {
         lockPromise2.lock();
         AssertJUnit.fail();
      } catch (TimeoutException e) {
         //expected!
      }
      lock.release(lockOwner1);
      AssertJUnit.assertFalse(lock.isFree());

      AssertJUnit.assertTrue(lockPromise3.isAvailable());
      lockPromise3.lock();
      AssertJUnit.assertEquals(lockOwner3, lock.getLockOwner());
      lock.release(lockOwner3);
      AssertJUnit.assertTrue(lock.isFree());

      //no side effects
      lock.release(lockOwner2);
      AssertJUnit.assertTrue(lock.isFree());
      AssertJUnit.assertNull(lock.getLockOwner());
   }

   public void testTimeout3() throws InterruptedException {
      final String lockOwner1 = "LO1";
      final String lockOwner2 = "LO2";
      final String lockOwner3 = "LO3";

      final InfinispanLock lock = new InfinispanLock(AbstractCacheTest.TIME_SERVICE);
      final LockPromise lockPromise1 = lock.acquire(lockOwner1, 0, TimeUnit.MILLISECONDS);
      final LockPromise lockPromise2 = lock.acquire(lockOwner2, 1, TimeUnit.DAYS);
      final LockPromise lockPromise3 = lock.acquire(lockOwner3, 1, TimeUnit.DAYS);

      AssertJUnit.assertTrue(lockPromise1.isAvailable());
      AssertJUnit.assertFalse(lockPromise2.isAvailable());
      AssertJUnit.assertFalse(lockPromise3.isAvailable());

      lockPromise1.lock();
      AssertJUnit.assertEquals(lockOwner1, lock.getLockOwner());

      //premature release. the lock is never acquired by owner 2
      //when the owner 1 releases, owner 3 is able to acquire it
      lock.release(lockOwner2);
      AssertJUnit.assertFalse(lock.isFree());
      AssertJUnit.assertEquals(lockOwner1, lock.getLockOwner());

      lock.release(lockOwner1);
      AssertJUnit.assertFalse(lock.isFree());

      AssertJUnit.assertTrue(lockPromise3.isAvailable());
      lockPromise3.lock();
      AssertJUnit.assertEquals(lockOwner3, lock.getLockOwner());
      lock.release(lockOwner3);
      AssertJUnit.assertTrue(lock.isFree());

      //no side effects
      lock.release(lockOwner2);
      AssertJUnit.assertTrue(lock.isFree());
      AssertJUnit.assertNull(lock.getLockOwner());
   }

   public void testSingleCounter() throws ExecutionException, InterruptedException {
      final NotThreadSafeCounter counter = new NotThreadSafeCounter();
      final InfinispanLock counterLock = new InfinispanLock(AbstractCacheTest.TIME_SERVICE);
      final int numThreads = 8;
      final int maxCounterValue = 100;
      final ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
      final CyclicBarrier barrier = new CyclicBarrier(numThreads);
      List<Future<Collection<Integer>>> callableResults = new ArrayList<>(numThreads);

      for (int i = 0; i < numThreads; ++i) {
         callableResults.add(executorService.submit(() -> {
            final Thread lockOwner = Thread.currentThread();
            AssertJUnit.assertEquals(0, counter.getCount());
            List<Integer> seenValues = new LinkedList<>();
            barrier.await();
            while (true) {
               counterLock.acquire(lockOwner, 1, TimeUnit.DAYS).lock();
               AssertJUnit.assertEquals(lockOwner, counterLock.getLockOwner());
               try {
                  int value = counter.getCount();
                  if (value == maxCounterValue) {
                     return seenValues;
                  }
                  seenValues.add(value);
                  counter.setCount(value + 1);
               } finally {
                  counterLock.release(lockOwner);
               }
            }
         }));
      }

      Set<Integer> seenResults = new HashSet<>();
      try {
         for (Future<Collection<Integer>> future : callableResults) {
            for (Integer integer : future.get()) {
               AssertJUnit.assertTrue(seenResults.add(integer));
            }
         }
      } finally {
         executorService.shutdown();
         executorService.awaitTermination(30, TimeUnit.SECONDS);
      }
      AssertJUnit.assertEquals(maxCounterValue, seenResults.size());
      for (int i = 0; i < maxCounterValue; ++i) {
         AssertJUnit.assertTrue(seenResults.contains(i));
      }
   }

   private static class NotThreadSafeCounter {
      private int count;

      public int getCount() {
         return count;
      }

      public void setCount(int count) {
         this.count = count;
      }
   }

}
