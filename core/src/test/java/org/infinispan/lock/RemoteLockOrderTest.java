package org.infinispan.lock;

import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.util.StrippedHashFunction;
import org.infinispan.util.concurrent.BlockingTaskAwareExecutorService;
import org.infinispan.util.concurrent.locks.order.LockLatch;
import org.infinispan.util.concurrent.locks.order.PerEntryRemoteLockOrderManager;
import org.infinispan.util.concurrent.locks.order.RemoteLockCommand;
import org.infinispan.util.concurrent.locks.order.RemoteLockOrderManager;
import org.infinispan.util.concurrent.locks.order.StripedRemoteLockOrderManager;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@Test(groups = "unit", testName = "lock.RemoteLockOrderTest")
public class RemoteLockOrderTest extends AbstractInfinispanTest {

   public void testSingleLatchReadyPerEntry() {
      final int numberRequest = 64;
      PerEntryRemoteLockOrderManager lockOrderManager = new PerEntryRemoteLockOrderManager();
      AtomicInteger integer = new AtomicInteger(0);
      lockOrderManager.init(mockConfiguration(AnyEquivalence.getInstance()),
                            mockClusteringDependentLogic(true),
                            mockExecutorService(integer));
      doSingleThreadTest(lockOrderManager, numberRequest);
      AssertJUnit.assertEquals(numberRequest - 1, integer.get());
      AssertJUnit.assertTrue(lockOrderManager.isEmpty());
   }

   public void testSingleLatchReadyStriped() {
      final int numberRequest = 64;
      StripedRemoteLockOrderManager lockOrderManager = new StripedRemoteLockOrderManager();
      AtomicInteger integer = new AtomicInteger(0);
      lockOrderManager.init(mockConfiguration(AnyEquivalence.getInstance()),
                            mockClusteringDependentLogic(true),
                            mockExecutorService(integer));
      doSingleThreadTest(lockOrderManager, numberRequest);
      AssertJUnit.assertEquals(numberRequest - 1, integer.get());
      AssertJUnit.assertTrue(lockOrderManager.isEmpty());
   }

   public void testConcurrentInsertAndReleasePerEntry() throws ExecutionException, InterruptedException {
      final PerEntryRemoteLockOrderManager lockOrderManager = new PerEntryRemoteLockOrderManager();
      final AtomicInteger integer = new AtomicInteger(0);
      lockOrderManager.init(mockConfiguration(AnyEquivalence.getInstance()),
                            mockClusteringDependentLogic(true),
                            mockExecutorService(integer));
      doConcurrentThreadTest(lockOrderManager, 1024);
      AssertJUnit.assertTrue(lockOrderManager.isEmpty());
   }

   public void testConcurrentInsertAndReleaseStriped() throws ExecutionException, InterruptedException {
      final StripedRemoteLockOrderManager lockOrderManager = new StripedRemoteLockOrderManager();
      final AtomicInteger integer = new AtomicInteger(0);
      lockOrderManager.init(mockConfiguration(AnyEquivalence.getInstance()),
                            mockClusteringDependentLogic(true),
                            mockExecutorService(integer));
      doConcurrentThreadTest(lockOrderManager, 1024);
      AssertJUnit.assertTrue(lockOrderManager.isEmpty());
   }

   public void testNoLockOwnerPerEntry() {
      final PerEntryRemoteLockOrderManager lockOrderManager = new PerEntryRemoteLockOrderManager();
      lockOrderManager.init(mockConfiguration(AnyEquivalence.getInstance()),
                            mockClusteringDependentLogic(false),
                            mockExecutorService(new AtomicInteger()));
      doNoLockOwnerTest(lockOrderManager);
   }

   public void testNoLockOwnerStriped() {
      final StripedRemoteLockOrderManager lockOrderManager = new StripedRemoteLockOrderManager();
      lockOrderManager.init(mockConfiguration(AnyEquivalence.getInstance()),
                            mockClusteringDependentLogic(false),
                            mockExecutorService(new AtomicInteger()));
      doNoLockOwnerTest(lockOrderManager);
   }

   public void testSimpleMultipleKeyPerEntry() {
      final PerEntryRemoteLockOrderManager lockOrderManager = new PerEntryRemoteLockOrderManager();
      Configuration configuration = mockConfiguration(AnyEquivalence.getInstance());
      lockOrderManager.init(configuration,
                            mockClusteringDependentLogic(true),
                            mockExecutorService(new AtomicInteger()));
      doSimpleMultipleLockTest(lockOrderManager, configuration);
      AssertJUnit.assertTrue(lockOrderManager.isEmpty());
   }

   public void testSimpleMultipleKeyStriped() {
      final StripedRemoteLockOrderManager lockOrderManager = new StripedRemoteLockOrderManager();
      Configuration configuration = mockConfiguration(AnyEquivalence.getInstance());
      lockOrderManager.init(configuration,
                            mockClusteringDependentLogic(true),
                            mockExecutorService(new AtomicInteger()));
      doSimpleMultipleLockTest(lockOrderManager, configuration);
      AssertJUnit.assertTrue(lockOrderManager.isEmpty());
   }

   private void doSimpleMultipleLockTest(RemoteLockOrderManager lockOrderManager, Configuration configuration) {
      List<Object> keys = findObjects(3, StrippedHashFunction.buildFromConfiguration(configuration));
      RemoteLockCommand command = mockCommand(Collections.singleton(keys.get(0)));
      LockLatch lockLatch = lockOrderManager.order(command);

      command = mockCommand(keys);
      LockLatch lockLatch2 = lockOrderManager.order(command);

      command = mockCommand(Collections.singleton(keys.get(1)));
      LockLatch lockLatch3 = lockOrderManager.order(command);

      command = mockCommand(Collections.singleton(keys.get(2)));
      LockLatch lockLatch4 = lockOrderManager.order(command);

      AssertJUnit.assertTrue(lockLatch.isReady());
      AssertJUnit.assertFalse(lockLatch2.isReady());
      AssertJUnit.assertFalse(lockLatch3.isReady());
      AssertJUnit.assertFalse(lockLatch4.isReady());

      lockLatch.release();

      AssertJUnit.assertTrue(lockLatch2.isReady());
      AssertJUnit.assertFalse(lockLatch3.isReady());
      AssertJUnit.assertFalse(lockLatch4.isReady());

      lockLatch2.release();

      AssertJUnit.assertTrue(lockLatch3.isReady());
      AssertJUnit.assertTrue(lockLatch4.isReady());

      lockLatch3.release();

      AssertJUnit.assertTrue(lockLatch4.isReady());

      lockLatch4.release();
   }

   private void doSingleThreadTest(RemoteLockOrderManager lockOrderManager, int numberRequest) {
      final List<LockLatch> list = new ArrayList<>(numberRequest);
      final RemoteLockCommand command = mockCommand(Collections.<Object>singleton("key"));
      for (int i = 0; i < numberRequest; i++) {
         list.add(lockOrderManager.order(command));
      }
      for (int j = 0; j < numberRequest; ++j) {
         AssertJUnit.assertTrue(list.get(j).isReady());
         for (int i = j + 1; i < numberRequest; ++i) {
            AssertJUnit.assertFalse(list.get(i).isReady());
         }
         list.get(j).release();
      }
   }

   private void doConcurrentThreadTest(final RemoteLockOrderManager lockOrderManager, final int numberRequest) throws ExecutionException, InterruptedException {
      final BlockingDeque<LockLatch> list = new LinkedBlockingDeque<>();
      final CyclicBarrier barrier = new CyclicBarrier(2);

      Future<Void> insert = fork(new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            barrier.await();
            RemoteLockCommand command = mockCommand(Collections.<Object>singleton("key"));
            for (int i = 0; i < numberRequest; ++i) {
               list.add(lockOrderManager.order(command));
            }
            return null;
         }
      });

      Future<Void> release = fork(new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            barrier.await();
            for (int i = 0; i < numberRequest; ++i) {
               LockLatch lockLatch = list.take();
               AssertJUnit.assertTrue(lockLatch.isReady());
               lockLatch.release();
            }
            return null;
         }
      });
      insert.get();
      release.get();
   }

   private void doNoLockOwnerTest(RemoteLockOrderManager lockOrderManager) {
      AssertJUnit.assertSame(LockLatch.NO_OP, lockOrderManager.order(mockCommand(Collections.<Object>singleton("key"))));
   }

   private List<Object> findObjects(int size, StrippedHashFunction<Object> hashFunction) {
      List<Object> objectList = new ArrayList<>(size);
      Set<Integer> usedSegments = new HashSet<>();
      for (int i = 0; i < size; ++i) {
         for (int j = 0; j < Integer.MAX_VALUE; ++j) {
            Object obj = new HashObject(j);
            int segment = hashFunction.hashToSegment(obj);
            if (usedSegments.add(segment)) {
               objectList.add(obj);
               break;
            }
         }
      }
      return objectList;
   }

   private static class HashObject {
      private final int hashCode;

      private HashObject(int hashCode) {
         this.hashCode = hashCode;
      }

      @Override
      public int hashCode() {
         return hashCode;
      }
   }

   private Configuration mockConfiguration(Equivalence<Object> equivalence) {
      Configuration configuration = Mockito.mock(Configuration.class, Mockito.RETURNS_DEEP_STUBS);
      Mockito.when(configuration.dataContainer().keyEquivalence()).thenReturn(equivalence);
      Mockito.when(configuration.locking().concurrencyLevel()).thenReturn(32);
      return configuration;
   }

   private ClusteringDependentLogic mockClusteringDependentLogic(boolean primaryOwner) {
      ClusteringDependentLogic logic = Mockito.mock(ClusteringDependentLogic.class);
      Mockito.when(logic.localNodeIsPrimaryOwner(Matchers.anyObject())).thenReturn(primaryOwner);
      return logic;
   }

   private BlockingTaskAwareExecutorService mockExecutorService(final AtomicInteger count) {
      BlockingTaskAwareExecutorService executorService = Mockito.mock(BlockingTaskAwareExecutorService.class);
      Mockito.doAnswer(new Answer() {
         @Override
         public Object answer(InvocationOnMock invocation) throws Throwable {
            count.incrementAndGet();
            return null;
         }
      }).when(executorService).checkForReadyTasks();
      return executorService;
   }

   private RemoteLockCommand mockCommand(Collection<Object> keys) {
      RemoteLockCommand command = Mockito.mock(RemoteLockCommand.class);
      Mockito.when(command.getKeysToLock()).thenReturn(keys);
      return command;
   }

}
