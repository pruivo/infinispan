package org.infinispan.util;

import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.transaction.xa.DldGlobalTransaction;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.locks.DeadlockDetectedException;
import org.infinispan.util.concurrent.locks.DeadlockDetectingLockManager;
import org.infinispan.util.concurrent.locks.LockContainer;
import org.infinispan.util.concurrent.locks.impl.PerKeyLockContainer;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.fail;

/**
 * Tests functionality in {@link org.infinispan.util.concurrent.locks.DeadlockDetectingLockManager}.
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "unit", testName = "util.DeadlockDetectingLockManagerTest")
public class DeadlockDetectingLockManagerTest extends AbstractInfinispanTest {

   private static final long SPIN_DURATION = 1000;
   private DeadlockDetectingLockManagerMock lockManager;

   @BeforeMethod
   public void setUp() {
      lockManager = new DeadlockDetectingLockManagerMock(createLockContainer(), createConfiguration());
      lockManager.init();
   }

   public void testNoTransaction() throws Exception {
      lockManager.lock("k", "aThread", 0, TimeUnit.MILLISECONDS).lock();
      try {
         lockManager.lock("k", "anotherThread", 0, TimeUnit.MILLISECONDS).lock();
         fail("TimeoutException expected!");
      } catch (TimeoutException e) {
         //expected
      }
   }

   public void testLockHeldByThread() throws Exception {
      lockManager.lock("k", "aThread", 0, TimeUnit.MILLISECONDS).lock();
      try {
         lockManager.lock("k", new DldGlobalTransaction(), 1000, TimeUnit.MILLISECONDS).lock();
         fail("TimeoutException expected!");
      } catch (TimeoutException e) {
         //expected
      }

      AssertJUnit.assertTrue(lockManager.getOverlapWithNotDeadlockAwareLockOwners() > 0); //it's not deterministic. can be 1 or more.
   }

   public void testLocalDeadlock() throws Exception {
      final DldGlobalTransaction ddgt = new DldGlobalTransaction();
      final DldGlobalTransaction lockOwner = new DldGlobalTransaction();

      ddgt.setCoinToss(0);
      lockOwner.setCoinToss(1);
      lockOwner.setRemote(false);
      lockOwner.setLockIntention(Collections.singleton("k"));
      AssertJUnit.assertTrue(ddgt.wouldLose(lockOwner));

      lockManager.setOwner(lockOwner);
      lockManager.setOwnsLock(true);

      lockManager.lock("k", lockOwner, 0, TimeUnit.MILLISECONDS).lock();

      try {
         lockManager.lock("k", ddgt, 1000, TimeUnit.MILLISECONDS).lock();
         assert false;
      } catch (DeadlockDetectedException e) {
         //expected
      }
      AssertJUnit.assertEquals(1, lockManager.getDetectedLocalDeadlocks());
   }

   private Configuration createConfiguration() {
      Configuration configuration = mock(Configuration.class, RETURNS_DEEP_STUBS);
      when(configuration.deadlockDetection().spinDuration()).thenReturn(SPIN_DURATION);
      when(configuration.jmxStatistics().enabled()).thenReturn(true);
      return configuration;
   }

   private LockContainer createLockContainer() {
      PerKeyLockContainer lockContainer = new PerKeyLockContainer(32, AnyEquivalence.getInstance());
      lockContainer.inject(TIME_SERVICE);
      return lockContainer;
   }

   private InvocationContext buildLocalTxIc(final DldGlobalTransaction ddgt) {
      return new LocalTxInvocationContext(mock(LocalTransaction.class)) {
         @Override
         public Object getLockOwner() {
            return ddgt;
         }
      };
   }

   public static class DeadlockDetectingLockManagerMock extends DeadlockDetectingLockManager {

      private Object owner;
      private boolean ownsLock;

      public DeadlockDetectingLockManagerMock(LockContainer lockContainer, Configuration configuration) {
         super.container = lockContainer;
         this.configuration = configuration;
         this.scheduler = Executors.newSingleThreadScheduledExecutor();
      }

      public void setOwner(Object owner) {
         this.owner = owner;
      }

      public void setOwnsLock(boolean ownsLock) {
         this.ownsLock = ownsLock;
      }

      @Override
      public Object getOwner(Object key) {
         return owner;
      }

      @Override
      public boolean ownsLock(Object key, Object owner) {
         return ownsLock;
      }
   }
}
