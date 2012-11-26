package org.infinispan.transaction.gmu.manager;

import org.infinispan.Cache;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.container.versioning.gmu.GMUVersion;
import org.infinispan.container.versioning.gmu.GMUVersionGenerator;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.transaction.gmu.CommitLog;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.concurrent.BlockingTaskAwareExecutorService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.infinispan.transaction.gmu.GMUHelper.toGMUVersion;
import static org.infinispan.transaction.gmu.GMUHelper.toGMUVersionGenerator;
import static org.infinispan.transaction.gmu.manager.SortedTransactionQueue.TransactionEntry;

/**
 * @author Pedro Ruivo
 * @author Sebastiano Peluso
 * @since 5.2
 */
public class TransactionCommitManager {

   //private CommitThread commitThread;
   private final SortedTransactionQueue sortedTransactionQueue;
   private long lastPreparedVersion = 0;
   private GMUVersionGenerator versionGenerator;
   private CommitLog commitLog;
   private GarbageCollectorManager garbageCollectorManager;
   private BlockingTaskAwareExecutorService gmuExecutor;

   public TransactionCommitManager() {
      sortedTransactionQueue = new SortedTransactionQueue();
   }

   @Inject
   public void inject(InvocationContextContainer icc, VersionGenerator versionGenerator, CommitLog commitLog,
                      Transport transport, Cache cache, GarbageCollectorManager garbageCollectorManager,
                      @ComponentName(value = KnownComponentNames.GMU_EXECUTOR) BlockingTaskAwareExecutorService gmuExecutor) {
      if (versionGenerator instanceof GMUVersionGenerator) {
         this.versionGenerator = toGMUVersionGenerator(versionGenerator);
      }
      this.commitLog = commitLog;
      this.garbageCollectorManager = garbageCollectorManager;
      this.gmuExecutor = gmuExecutor;
   }

   /**
    * add a transaction to the queue. A temporary commit vector clock is associated and with it, it order the
    * transactions
    *
    * @param cacheTransaction the transaction to be prepared
    */
   public synchronized void prepareTransaction(CacheTransaction cacheTransaction) {
      long concurrentClockNumber = commitLog.getCurrentVersion().getThisNodeVersionValue();
      EntryVersion preparedVersion = versionGenerator.setNodeVersion(commitLog.getCurrentVersion(),
                                                                     ++lastPreparedVersion);

      cacheTransaction.setTransactionVersion(preparedVersion);
      sortedTransactionQueue.prepare(cacheTransaction,concurrentClockNumber);
   }

   public void rollbackTransaction(CacheTransaction cacheTransaction) {
      sortedTransactionQueue.rollback(cacheTransaction);if (sortedTransactionQueue.hasTransactionReadyToCommit()) {
         //is it safe to do it in this thread?
         gmuExecutor.checkForReadyTasks();
      }

   }

   public synchronized TransactionEntry commitTransaction(GlobalTransaction globalTransaction, EntryVersion version) {
      GMUVersion commitVersion = toGMUVersion(version);
      lastPreparedVersion = Math.max(commitVersion.getThisNodeVersionValue(), lastPreparedVersion);
      TransactionEntry entry = sortedTransactionQueue.commit(globalTransaction, commitVersion);
      if (entry == null) {
         commitLog.updateMostRecentVersion(commitVersion);
      }
      if (sortedTransactionQueue.hasTransactionReadyToCommit()) {
         //is it safe to do it in this thread?
         gmuExecutor.checkForReadyTasks();
      }
      return entry;
   }

   public void prepareReadOnlyTransaction(CacheTransaction cacheTransaction) {
      EntryVersion preparedVersion = commitLog.getCurrentVersion();
      cacheTransaction.setTransactionVersion(preparedVersion);
   }

   public Collection<TransactionEntry> getTransactionsToCommit() {
      List<TransactionEntry> transactionEntries = new ArrayList<TransactionEntry>(4);
      sortedTransactionQueue.populateToCommit(transactionEntries);
      return transactionEntries;
   }

   public void transactionCommitted(Collection<CommittedTransaction> transactions) {
      commitLog.insertNewCommittedVersions(transactions);
      garbageCollectorManager.notifyCommittedTransactions(transactions.size());
      if (sortedTransactionQueue.hasTransactionReadyToCommit()) {
         //is it safe to do it in this thread?
         gmuExecutor.checkForReadyTasks();
      }
   }

   //DEBUG ONLY!
   public final TransactionEntry getTransactionEntry(GlobalTransaction globalTransaction) {
      return sortedTransactionQueue.getTransactionEntry(globalTransaction);
   }

   /*private class CommitThread extends Thread {
      private final List<CommittedTransaction> committedTransactions;
      private final List<SortedTransactionQueue.TransactionEntry> commitList;
      private boolean running;

      private CommitThread(String threadName) {
         super(threadName);
         running = false;
         committedTransactions = new LinkedList<CommittedTransaction>();
         commitList = new LinkedList<SortedTransactionQueue.TransactionEntry>();
      }

      @Override
      public void run() {
         running = true;
         while (running) {
            try {
               sortedTransactionQueue.populateToCommit(commitList);
               if (commitList.isEmpty()) {
                  continue;
               }

               int subVersion = 0;
               for (TransactionEntry transactionEntry : commitList) {
                  try {
                     if (log.isTraceEnabled()) {
                        log.tracef("Committing transaction entries for %s", transactionEntry);
                     }

                     CacheTransaction cacheTransaction = transactionEntry.getCacheTransactionForCommit();

                     CommittedTransaction committedTransaction = new CommittedTransaction(cacheTransaction, subVersion);
                     commitInvocationInstance.commitTransaction(createInvocationContext(cacheTransaction, subVersion));
                     committedTransactions.add(committedTransaction);

                     if (log.isTraceEnabled()) {
                        log.tracef("Transaction entries committed for %s", transactionEntry);
                     }
                  } catch (Exception e) {
                     log.warnf("Error occurs while committing transaction entries for %s", transactionEntry);
                  } finally {
                     icc.clearThreadLocal();
                     subVersion++;
                     //garbageCollectorManager.notifyCommittedTransaction();
                  }
               }

               commitLog.insertNewCommittedVersions(committedTransactions);
            } catch (InterruptedException e) {
               running = false;
               if (log.isTraceEnabled()) {
                  log.tracef("%s was interrupted", getName());
               }
               this.interrupt();
            } catch (Throwable throwable) {
               log.fatalf(throwable, "Exception caught in commit. This should not happen");
            } finally {
               for (TransactionEntry transactionEntry : commitList) {
                  transactionEntry.committed();
               }
               committedTransactions.clear();
               commitList.clear();
            }
         }
      }

      @Override
      public void interrupt() {
         running = false;
         super.interrupt();
      }

      private TxInvocationContext createInvocationContext(CacheTransaction cacheTransaction, int subVersion) {
         GMUCacheEntryVersion cacheEntryVersion = versionGenerator.convertVersionToWrite(cacheTransaction.getTransactionVersion(),
                                                                                         subVersion);
         cacheTransaction.setTransactionVersion(cacheEntryVersion);
         if (cacheTransaction instanceof LocalTransaction) {
            LocalTxInvocationContext localTxInvocationContext = icc.createTxInvocationContext();
            localTxInvocationContext.setLocalTransaction((LocalTransaction) cacheTransaction);
            return localTxInvocationContext;
         } else if (cacheTransaction instanceof RemoteTransaction) {
            return icc.createRemoteTxInvocationContext((RemoteTransaction) cacheTransaction, null);
         }
         throw new IllegalStateException("Expected a remote or local transaction and not " + cacheTransaction);
      }
   }*/
}
