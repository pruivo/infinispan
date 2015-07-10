package org.infinispan.util;

import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.context.impl.NonTxInvocationContext;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.transaction.xa.DldGlobalTransaction;
import org.infinispan.transaction.xa.TransactionFactory;
import org.infinispan.util.concurrent.locks.DeadlockDetectedException;
import org.infinispan.util.concurrent.locks.DeadlockDetectingLockManager;
import org.infinispan.util.concurrent.locks.LockContainer;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

/**
 * Tests functionality in {@link org.infinispan.util.concurrent.locks.DeadlockDetectingLockManager}.
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "unit", testName = "util.DeadlockDetectingLockManagerTest")
public class DeadlockDetectingLockManagerTest extends AbstractInfinispanTest {

   private static final int SPIN_DURATION = 1000;
   DeadlockDetectingLockManagerMock lockManager;
   Configuration config = new ConfigurationBuilder().build();
   private LockContainer lc;
   private DldGlobalTransaction lockOwner;

   @BeforeMethod
   public void setUp() {
      lc = mock(LockContainer.class);
      lockManager = new DeadlockDetectingLockManagerMock(true, lc, config);
      /*LockContainer container = extractField(lockManager, "container");
      if (container instanceof PerKeyLockContainer) {
         ((PerKeyLockContainer) container).inject(TIME_SERVICE);
      } else if (container instanceof StripedLockContainer) {
         ((StripedLockContainer) container).inject(TIME_SERVICE);
      }*/
      lockOwner = (DldGlobalTransaction) TransactionFactory.TxFactoryEnum.DLD_NORECOVERY_XA.newGlobalTransaction();
   }


   public void testNoTransaction() throws Exception {
      InvocationContext nonTx = new NonTxInvocationContext(AnyEquivalence.getInstance());

      Lock mockLock = mock(Lock.class);
      when(lc.acquireLock(nonTx.getLockOwner(), "k", config.locking().lockAcquisitionTimeout(), TimeUnit.MILLISECONDS)).thenReturn(mockLock).thenReturn(null);

      assert lockManager.lockAndRecord("k", nonTx, config.locking().lockAcquisitionTimeout());
      assert !lockManager.lockAndRecord("k", nonTx, config.locking().lockAcquisitionTimeout());
   }

   public void testLockHeldByThread() throws Exception {
      InvocationContext localTxContext = buildLocalTxIc(new DldGlobalTransaction());

      Lock mockLock = mock(Lock.class);
      //this makes sure that we cannot acquire lock from the first try
      when(lc.acquireLock(localTxContext.getLockOwner(), "k", SPIN_DURATION, TimeUnit.MILLISECONDS)).thenReturn(null).thenReturn(mockLock);
      lockManager.setOwner(Thread.currentThread());
      //next lock acquisition will succeed

      assert lockManager.lockAndRecord("k", localTxContext, config.locking().lockAcquisitionTimeout());
      assert lockManager.getOverlapWithNotDeadlockAwareLockOwners() == 1;
   }

   public void testLocalDeadlock() throws Exception {
      final DldGlobalTransaction ddgt = (DldGlobalTransaction) TransactionFactory.TxFactoryEnum.DLD_NORECOVERY_XA.newGlobalTransaction();

      InvocationContext localTxContext = buildLocalTxIc(ddgt);

      ddgt.setCoinToss(0);
      lockOwner.setCoinToss(1);
      assert ddgt.wouldLose(lockOwner);

      //this makes sure that we cannot acquire lock from the first try
      Lock mockLock = mock(Lock.class);
      when(lc.acquireLock(localTxContext.getLockOwner(), "k", SPIN_DURATION, TimeUnit.MILLISECONDS)).thenReturn(null).thenReturn(mockLock);
      lockOwner.setRemote(false);
      lockOwner.setLockIntention("k");
      lockManager.setOwner(lockOwner);
      lockManager.setOwnsLock(true);
      try {
         lockManager.lockAndRecord("k", localTxContext, config.locking().lockAcquisitionTimeout());
         assert false;
      } catch (DeadlockDetectedException e) {
         //expected
      }
      assertEquals(1l, lockManager.getDetectedLocalDeadlocks());
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

      public DeadlockDetectingLockManagerMock(boolean exposeJmxStats, LockContainer lockContainer, Configuration configuration) {
         this.exposeJmxStats = exposeJmxStats;
         super.container = lockContainer;
         this.configuration = configuration;
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
