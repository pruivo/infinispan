package org.infinispan.lock;

import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.test.AbstractCacheTest;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.locks.LockContainerV8;
import org.infinispan.util.concurrent.locks.LockPromise;
import org.infinispan.util.concurrent.locks.impl.PerKeyLockContainer;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
@Test(groups = "unit", testName = "lock.LockContainerV8Test")
public class LockContainerV8Test {

   public void testSingleLockWithPerEntry() throws InterruptedException {
      PerKeyLockContainer lockContainer = new PerKeyLockContainer(AnyEquivalence.getInstance());
      lockContainer.inject(AbstractCacheTest.TIME_SERVICE);
      doTestWithSingleLock(lockContainer, -1);
   }

   private void doTestWithSingleLock(LockContainerV8 container, int poolSize) throws InterruptedException {
      final String lockOwner1 = "LO1";
      final String lockOwner2 = "LO2";
      final String lockOwner3 = "LO3";

      final LockPromise lockPromise1 = container.get("key").acquire(lockOwner1, 0, TimeUnit.MILLISECONDS);
      final LockPromise lockPromise2 = container.get("key").acquire(lockOwner2, 0, TimeUnit.MILLISECONDS);
      final LockPromise lockPromise3 = container.get("key").acquire(lockOwner3, 0, TimeUnit.MILLISECONDS);

      AssertJUnit.assertEquals(1, container.getNumLocksHeld());
      if (poolSize == -1) {
         //dynamic
         AssertJUnit.assertEquals(1, container.size());
      } else {
         AssertJUnit.assertEquals(poolSize, container.size());
      }

      acquireLock(lockPromise1, false);
      acquireLock(lockPromise2, true);
      acquireLock(lockPromise3, true);

      AssertJUnit.assertEquals(1, container.getNumLocksHeld());
      if (poolSize == -1) {
         //dynamic
         AssertJUnit.assertEquals(1, container.size());
      } else {
         AssertJUnit.assertEquals(poolSize, container.size());
      }

      container.peek("key").release(lockOwner2);
      container.peek("key").release(lockOwner3);

      AssertJUnit.assertEquals(1, container.getNumLocksHeld());
      if (poolSize == -1) {
         //dynamic
         AssertJUnit.assertEquals(1, container.size());
      } else {
         AssertJUnit.assertEquals(poolSize, container.size());
      }

      container.peek("key").release(lockOwner1);

      AssertJUnit.assertEquals(0, container.getNumLocksHeld());
      if (poolSize == -1) {
         //dynamic
         AssertJUnit.assertEquals(0, container.size());
      } else {
         AssertJUnit.assertEquals(poolSize, container.size());
      }
   }

   private void acquireLock(LockPromise promise, boolean timeout) throws InterruptedException {
      try {
         promise.lock();
         AssertJUnit.assertFalse(timeout);
      } catch (TimeoutException e) {
         AssertJUnit.assertTrue(timeout);
      }
   }

}
