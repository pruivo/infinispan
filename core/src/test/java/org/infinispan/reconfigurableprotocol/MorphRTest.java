package org.infinispan.reconfigurableprotocol;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.reconfigurableprotocol.manager.ProtocolManager;
import org.infinispan.reconfigurableprotocol.manager.ReconfigurableReplicationManager;
import org.infinispan.reconfigurableprotocol.protocol.PassiveReplicationCommitProtocol;
import org.infinispan.reconfigurableprotocol.protocol.TotalOrderCommitProtocol;
import org.infinispan.reconfigurableprotocol.protocol.TwoPhaseCommitProtocol;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.PassiveReplicationException;
import org.infinispan.transaction.TransactionProtocol;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;

/**
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", testName = "reconfigurableprotocol.MorphRTest")
@CleanupAfterMethod
public class MorphRTest extends MultipleCacheManagersTest {

   private static final int MAX_KEYS = 256;
   private static final int MAX_ACCESSES = 15;
   private static final int MIN_ACCESSES = 5;
   private static final int NUM_CACHES = 4;
   private final Worker[] workers = new Worker[NUM_CACHES];
   private final Thread[] threads = new Thread[NUM_CACHES];

   public void test2PC_PB() throws Exception {
      doSimpleReconfiguration(TwoPhaseCommitProtocol.UID, PassiveReplicationCommitProtocol.UID);
   }

   public void test2PC_TO() throws Exception {
      doSimpleReconfiguration(TwoPhaseCommitProtocol.UID, TotalOrderCommitProtocol.UID);
   }

   public void testPB_2PC() throws Exception {
      doSimpleReconfiguration(PassiveReplicationCommitProtocol.UID, TwoPhaseCommitProtocol.UID);
   }

   public void testPB_TO() throws Exception {
      doSimpleReconfiguration(PassiveReplicationCommitProtocol.UID, TotalOrderCommitProtocol.UID);
   }

   public void testTO_2PC() throws Exception {
      doSimpleReconfiguration(TotalOrderCommitProtocol.UID, TwoPhaseCommitProtocol.UID);
   }

   public void testTO_PB() throws Exception {
      doSimpleReconfiguration(TotalOrderCommitProtocol.UID, PassiveReplicationCommitProtocol.UID);
   }

   @AfterMethod(alwaysRun = true)
   public void killThreads() {
      removeDeadWorkers();
      interruptWorkers();
      for (Thread thread : threads) {
         safeJoin(thread, 10000);
      }
      removeDeadWorkers();
      interruptWorkers();
      for (Thread thread : threads) {
         safeJoin(thread, 10000);
      }
      for (int i = 0; i < NUM_CACHES; ++i) {
         threads[i] = null;
         workers[i] = null;
      }
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder defaultBuilder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      defaultBuilder.locking().isolationLevel(IsolationLevel.REPEATABLE_READ)
            .writeSkewCheck(false);
      defaultBuilder.clustering().stateTransfer().fetchInMemoryState(false);
      defaultBuilder.transaction().syncCommitPhase(true)
            .recovery().disable();
      decorate(defaultBuilder);
      for (int i = 0; i < NUM_CACHES; ++i) {
         addClusterEnabledCacheManager(defaultBuilder);
      }
      defineConfigurationOnAllManagers("2PC", defaultBuilder);
      ConfigurationBuilder TO = new ConfigurationBuilder().read(defaultBuilder.build());
      TO.transaction().transactionProtocol(TransactionProtocol.TOTAL_ORDER);
      defineConfigurationOnAllManagers("TO", TO);
      ConfigurationBuilder PB = new ConfigurationBuilder().read(defaultBuilder.build());
      PB.transaction().transactionProtocol(TransactionProtocol.PASSIVE_REPLICATION);
      defineConfigurationOnAllManagers("PB", PB);
   }

   protected void decorate(ConfigurationBuilder builder) {
      //no-op
   }

   private void amendCoolDownTime(Cache cache) {
      reconfigurableReplicationManager(cache).setSwitchCoolDownTime(5);
   }

   private ReconfigurableReplicationManager reconfigurableReplicationManager(Cache cache) {
      return TestingUtil.extractComponent(cache, ReconfigurableReplicationManager.class);
   }

   private void doSimpleReconfiguration(String from, String to) throws Exception {
      waitForClusterToForm(from);
      List<Cache<String, String>> caches = caches(from);
      amendCoolDownTime(caches.get(0));
      populate(caches);
      createWorkers(caches);

      awaitUntilIsAbleToSwitch(caches);
      triggerSwitch(caches.get(0), to);
      awaitUntilIsAbleToSwitch(caches);

      stopWorkers();
      joinWorkers(10000);
      removeDeadWorkers();

      if (hasWorkersAlive()) {
         log.warnf("There are still workers alive: %s", Arrays.toString(workers));
      }

      assertNoTransactions();
      assertNoTransactionInProtocol(caches);
   }

   private void assertNoTransactionInProtocol(final List<Cache<String, String>> caches) {
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            int pendingTx = 0;
            log.debug("Checking no transactions in protocol...");
            for (Cache cache : caches) {
               for (ReconfigurableProtocol protocol : reconfigurableReplicationManager(cache).getReconfigurableProtocols()) {
                  Collection<GlobalTransaction> localCommittingTx = protocol.getLocalTransactions();
                  Collection<GlobalTransaction> remoteCommittingTx = protocol.getRemoteTransactions();
                  Collection<Transaction> runningTxs = protocol.getExecutingTransactions();
                  log.debugf("%s: running=%s, local=%s, remote=%s", protocol.getUniqueProtocolName(), runningTxs,
                             localCommittingTx, remoteCommittingTx);
                  pendingTx += localCommittingTx.size() + remoteCommittingTx.size() + runningTxs.size();
               }
            }
            return pendingTx == 0;
         }
      }, 30000);
   }

   private void stopWorkers() throws InterruptedException {
      for (Worker worker : workers) {
         if (worker != null) {
            worker.stopWorker();
         }
      }
   }

   private void createWorkers(List<Cache<String, String>> caches) {
      for (int i = 0; i < NUM_CACHES; ++i) {
         workers[i] = new Worker(caches.get(i));
         threads[i] = fork(workers[i], false);
      }
   }

   private void joinWorkers(long timeout) throws InterruptedException {
      for (Thread thread : threads) {
         if (thread != null) {
            thread.join(timeout);
         }
      }
   }

   private void interruptWorkers() {
      for (Thread thread : threads) {
         if (thread != null) {
            thread.interrupt();
         }
      }
   }

   private void removeDeadWorkers() {
      for (int i = 0; i < threads.length; ++i) {
         if (threads[i] != null && !threads[i].isAlive()) {
            threads[i] = null;
            workers[i] = null;
         }
      }
   }

   private boolean hasWorkersAlive() {
      for (Thread thread : threads) {
         if (thread != null) {
            return true;
         }
      }
      return false;
   }

   private void safeJoin(Thread thread, long timeout) {
      if (thread == null) {
         return;
      }
      try {
         thread.join(timeout);
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         //ignored
      }
   }

   private void awaitUntilIsAbleToSwitch(final List<Cache<String, String>> caches) {
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            log.debug("Checking if it is able to switch...");
            boolean ready = true;
            for (Cache cache : caches) {
               ReconfigurableReplicationManager manager = reconfigurableReplicationManager(cache);
               boolean coolDownExpired = manager.isCoolDownTimeExpired();
               ProtocolManager.State state = manager.getState();
               log.debugf("%s: isCoolDownExpired? %s, State=%s", address(cache), coolDownExpired, state);
               ready = ready && coolDownExpired && state == ProtocolManager.State.SAFE;
            }
            return ready;
         }
      }, 30000);
   }

   private void triggerSwitch(Cache cache, String protocol, boolean stop, boolean abort) throws Exception {
      reconfigurableReplicationManager(cache).switchTo(protocol, stop, abort);
   }

   private void triggerSwitch(Cache cache, String protocol) throws Exception {
      triggerSwitch(cache, protocol, false, false);
   }

   private void populate(List<Cache<String, String>> caches) {
      for (Cache<String, String> cache : caches) {
         if (canWrite(cache)) {
            populate(cache);
            return;
         }
      }
      throw new IllegalStateException("Should never happen :)");
   }

   private void populate(Cache<String, String> cache) {
      for (int i = 0; i <= MAX_KEYS; ++i) {
         cache.put(key(i), value(i));
      }
   }

   private String key(int i) {
      return "key_" + Math.abs(i % MAX_KEYS);
   }

   private String value(int i) {
      return "value_" + i;
   }

   private boolean isMaster(Cache cache) {
      RpcManager rpcManager = cache.getAdvancedCache().getRpcManager();
      return rpcManager.getAddress().equals(rpcManager.getMembers().get(0));
   }

   private boolean canWrite(Cache cache) {
      return !reconfigurableReplicationManager(cache).getCurrentProtocolId().equals(PassiveReplicationCommitProtocol.UID) ||
            isMaster(cache);
   }

   private void safeRollback(TransactionManager transactionManager) {
      try {
         if (transactionManager.getStatus() != Status.STATUS_NO_TRANSACTION) {
            transactionManager.rollback();
         }
      } catch (SystemException e) {
         //ignore
      }
   }

   private class Worker implements Runnable {

      private final Cache<String, String> cache;
      private final TransactionManager transactionManager;
      private final Random random;
      private volatile boolean interrupted;

      private Worker(Cache<String, String> cache) {
         this.cache = cache;
         transactionManager = cache.getAdvancedCache().getTransactionManager();
         random = new Random(System.currentTimeMillis() << cache.hashCode());
         interrupted = false;
      }

      public void stopWorker() {
         interrupted = true;
      }

      @Override
      public void run() {
         while (!interrupted && !Thread.currentThread().isInterrupted()) {
            try {

               if (canWrite(cache) && random.nextInt(100) > 50) {
                  writeTransaction();
               } else {
                  readTransaction();
               }
            } catch (Exception unexpected) {
               log.errorf(unexpected, "Unexpected exception in %s", Thread.currentThread().getName());
               return;
            } finally {
               safeRollback(transactionManager);
            }
         }
      }

      private void readTransaction() throws Exception {
         int accesses = numberOfAccesses();
         int hashCode = random.nextInt();
         try {
            transactionManager.begin();
            while (accesses-- > 0) {
               hashCode = cache.get(key(hashCode)).hashCode();
            }
            transactionManager.commit();
         } catch (HeuristicRollbackException e) {
            //ignore
         } catch (RollbackException e) {
            //ignore
         } catch (HeuristicMixedException e) {
            //ignore
         } catch (PassiveReplicationException e) {
            //ignore
         }
      }

      private void writeTransaction() throws Exception {
         int accesses = numberOfAccesses();
         int hashCode = random.nextInt();
         try {
            transactionManager.begin();
            while (accesses-- > 0) {
               hashCode = cache.get(key(hashCode)).hashCode();
               cache.put(key(hashCode), value(hashCode));
            }
            transactionManager.commit();
         } catch (HeuristicRollbackException e) {
            //ignore
         } catch (RollbackException e) {
            //ignore
         } catch (HeuristicMixedException e) {
            //ignore
         } catch (PassiveReplicationException e) {
            //ignore
         }
      }

      private int numberOfAccesses() {
         return Math.max(MIN_ACCESSES, random.nextInt(MAX_ACCESSES));
      }
   }
}
