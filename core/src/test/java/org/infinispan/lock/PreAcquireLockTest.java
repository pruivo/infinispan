package org.infinispan.lock;

import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.util.concurrent.locks.LockPlaceHolder;
import org.infinispan.util.concurrent.locks.containers.AbstractLockContainer;
import org.infinispan.util.concurrent.locks.containers.LockContainer;
import org.infinispan.util.concurrent.locks.containers.OwnableReentrantPerEntryLockContainer;
import org.infinispan.util.concurrent.locks.containers.OwnableReentrantStripedLockContainer;
import org.infinispan.util.concurrent.locks.containers.ReentrantPerEntryLockContainer;
import org.infinispan.util.concurrent.locks.containers.ReentrantStripedLockContainer;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "unit", testName = "lock.PreAcquireLockTest")
public class PreAcquireLockTest extends AbstractInfinispanTest {

   private static final int CONCURRENCY_LEVEL = 32;

   public void testOwnablePerEntryNoContentionTest() throws ExecutionException, InterruptedException {
      doNoContentionTest(LockContainerType.OWNABLE_PER_ENTRY);
   }

   public void testOwnableStrippedNoContentionTest() throws ExecutionException, InterruptedException {
      doNoContentionTest(LockContainerType.OWNABLE_STRIPPED);
   }

   public void testJDKPerEntryNoContentionTest() throws ExecutionException, InterruptedException {
      doNoContentionTest(LockContainerType.JDK_PER_ENTRY);
   }

   public void testJDKStrippedNoContentionTest() throws ExecutionException, InterruptedException {
      doNoContentionTest(LockContainerType.JDK_STRIPPED);
   }

   public void testOwnablePerEntryContentionTest() throws ExecutionException, InterruptedException {
      doContentionTest(LockContainerType.OWNABLE_PER_ENTRY);
   }

   public void testOwnableStrippedContentionTest() throws ExecutionException, InterruptedException {
      doContentionTest(LockContainerType.OWNABLE_STRIPPED);
   }

   public void testJDKPerEntryContentionTest() throws ExecutionException, InterruptedException {
      doContentionTest(LockContainerType.JDK_PER_ENTRY);
   }

   public void testJDKStrippedContentionTest() throws ExecutionException, InterruptedException {
      doContentionTest(LockContainerType.JDK_STRIPPED);
   }

   private void doNoContentionTest(LockContainerType type) throws InterruptedException, ExecutionException {
      final LockContainer lockContainer = type.build();
      final Object[] keys = findDisjointKeys(2, lockContainer, "no-contention-key");
      log.tracef("Key set is %s", Arrays.toString(keys));
      final CyclicBarrier barrier = new CyclicBarrier(2);
      final Future<Void> f1 = fork(new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            LockPlaceHolder placeHolder = lockContainer.preAcquireLocks(this, 10, TimeUnit.MILLISECONDS, keys[0]);
            AssertJUnit.assertTrue(placeHolder.isReady());
            barrier.await();
            AssertJUnit.assertTrue(lockContainer.acquireLock(this, keys[0], 10, TimeUnit.MILLISECONDS));
            return null;
         }
      });

      final Future<Void> f2 = fork(new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            LockPlaceHolder placeHolder = lockContainer.preAcquireLocks(this, 10, TimeUnit.MILLISECONDS, keys[1]);
            AssertJUnit.assertTrue(placeHolder.isReady());
            barrier.await();
            AssertJUnit.assertTrue(lockContainer.acquireLock(this, keys[1], 10, TimeUnit.MILLISECONDS));
            return null;
         }
      });

      f1.get();
      f2.get();
   }

   private void doContentionTest(LockContainerType type) throws ExecutionException, InterruptedException {
      final AbstractLockContainer<?> lockContainer = type.build();
      final Object[] keys = findDisjointKeys(4, lockContainer, "contention-key");
      log.tracef("Key set is %s", Arrays.toString(keys));
      final CyclicBarrier barrier = new CyclicBarrier(3);

      final Future<Void> f1 = fork(new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            barrier.await();
            LockPlaceHolder holder = lockContainer.preAcquireLocks(this, 10, TimeUnit.MILLISECONDS, keys[0], keys[1]);
            barrier.await();
            barrier.await();
            barrier.await();
            boolean isReady = holder.isReady();
            barrier.await();
            AssertJUnit.assertTrue(isReady);
            holder.awaitReady();
            AssertJUnit.assertTrue(lockContainer.acquireLock(this, keys[0], 10, TimeUnit.MILLISECONDS));
            AssertJUnit.assertTrue(lockContainer.acquireLock(this, keys[1], 10, TimeUnit.MILLISECONDS));
            TimeUnit.SECONDS.sleep(1);
            lockContainer.releaseLock(this, keys[1]);
            lockContainer.releaseLock(this, keys[0]);
            return null;
         }
      });

      final Future<Void> f2 = fork(new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            barrier.await();
            barrier.await();
            LockPlaceHolder holder = lockContainer.preAcquireLocks(this, 30, TimeUnit.SECONDS, keys[1], keys[2]);
            barrier.await();
            barrier.await();
            boolean isReady = holder.isReady();
            barrier.await();
            AssertJUnit.assertFalse(isReady);
            holder.awaitReady();
            AssertJUnit.assertTrue(holder.isReady());
            AssertJUnit.assertTrue(lockContainer.acquireLock(this, keys[1], 10, TimeUnit.MILLISECONDS));
            AssertJUnit.assertTrue(lockContainer.acquireLock(this, keys[2], 10, TimeUnit.MILLISECONDS));
            TimeUnit.SECONDS.sleep(1);
            lockContainer.releaseLock(this, keys[2]);
            lockContainer.releaseLock(this, keys[1]);
            return null;
         }
      });

      final Future<Void> f3 = fork(new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            barrier.await();
            barrier.await();
            barrier.await();
            LockPlaceHolder holder = lockContainer.preAcquireLocks(this, 30, TimeUnit.SECONDS, keys[2], keys[3]);
            barrier.await();
            boolean isReady = holder.isReady();
            barrier.await();
            AssertJUnit.assertFalse(isReady);
            holder.awaitReady();
            AssertJUnit.assertTrue(holder.isReady());
            AssertJUnit.assertTrue(lockContainer.acquireLock(this, keys[2], 10, TimeUnit.MILLISECONDS));
            AssertJUnit.assertTrue(lockContainer.acquireLock(this, keys[3], 10, TimeUnit.MILLISECONDS));
            TimeUnit.SECONDS.sleep(1);
            lockContainer.releaseLock(this, keys[3]);
            lockContainer.releaseLock(this, keys[2]);
            return null;
         }
      });

      f1.get();
      f2.get();
      f3.get();

      AssertJUnit.assertEquals(0, lockContainer.getNumLocksHeld());
      AssertJUnit.assertTrue(lockContainer.isQueuesEmpty());
   }

   private static Object[] findDisjointKeys(int numberOfKeys, LockContainer lockContainer, String prefix) {
      Set<Integer> locksId = new HashSet<Integer>();
      Object[] keys = new Object[numberOfKeys];
      int keyId = 0;
      while (numberOfKeys > 0) {
         Object key = prefix + "_" + keyId++;
         int lockId = lockContainer.getLockId(key);
         if (lockId == -1 || locksId.add(lockId)) {
            keys[--numberOfKeys] = key;
         }
      }
      return keys;
   }

   private static enum LockContainerType {
      OWNABLE_PER_ENTRY {
         @Override
         AbstractLockContainer<?> build() {
            OwnableReentrantPerEntryLockContainer lockContainer =
                  new OwnableReentrantPerEntryLockContainer(CONCURRENCY_LEVEL, AnyEquivalence.getInstance());
            lockContainer.injectTimeService(TIME_SERVICE);
            return lockContainer;
         }
      },
      OWNABLE_STRIPPED {
         @Override
         AbstractLockContainer<?> build() {
            OwnableReentrantStripedLockContainer lockContainer =
                  new OwnableReentrantStripedLockContainer(CONCURRENCY_LEVEL, AnyEquivalence.getInstance());
            lockContainer.injectTimeService(TIME_SERVICE);
            return lockContainer;
         }
      },
      JDK_PER_ENTRY {
         @Override
         AbstractLockContainer<?> build() {
            ReentrantPerEntryLockContainer lockContainer = new ReentrantPerEntryLockContainer(CONCURRENCY_LEVEL,
                                                                                              AnyEquivalence.getInstance());
            lockContainer.injectTimeService(TIME_SERVICE);
            return lockContainer;
         }
      },
      JDK_STRIPPED {
         @Override
         AbstractLockContainer<?> build() {
            ReentrantStripedLockContainer lockContainer = new ReentrantStripedLockContainer(CONCURRENCY_LEVEL,
                                                                                            AnyEquivalence.getInstance());
            lockContainer.injectTimeService(TIME_SERVICE);
            return lockContainer;
         }
      };

      abstract AbstractLockContainer<?> build();
   }

}
