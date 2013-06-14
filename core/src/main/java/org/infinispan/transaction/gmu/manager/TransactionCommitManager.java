package org.infinispan.transaction.gmu.manager;

import org.infinispan.Cache;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.container.versioning.gmu.GMUVersion;
import org.infinispan.container.versioning.gmu.GMUVersionGenerator;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.transaction.gmu.CommitLog;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;

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

   public TransactionCommitManager() {
      sortedTransactionQueue = new SortedTransactionQueue();
   }

   @Inject
   public void inject(InvocationContextContainer icc, VersionGenerator versionGenerator, CommitLog commitLog,
                      Transport transport, Cache cache, GarbageCollectorManager garbageCollectorManager) {
      if (versionGenerator instanceof GMUVersionGenerator) {
         this.versionGenerator = toGMUVersionGenerator(versionGenerator);
      }
      this.commitLog = commitLog;
      this.garbageCollectorManager = garbageCollectorManager;
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
      sortedTransactionQueue.prepare(cacheTransaction, concurrentClockNumber);
   }

   public void rollbackTransaction(CacheTransaction cacheTransaction) {
      sortedTransactionQueue.rollback(cacheTransaction);
   }

   public synchronized TransactionEntry commitTransaction(GlobalTransaction globalTransaction, EntryVersion version) {
      GMUVersion commitVersion = toGMUVersion(version);
      lastPreparedVersion = Math.max(commitVersion.getThisNodeVersionValue(), lastPreparedVersion);
      TransactionEntry entry = sortedTransactionQueue.commit(globalTransaction, commitVersion);
      if (entry == null) {
         commitLog.updateMostRecentVersion(commitVersion);
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

   public void transactionCommitted(Collection<CommittedTransaction> transactions, Collection<TransactionEntry> transactionEntries) {
      commitLog.insertNewCommittedVersions(transactions);
      garbageCollectorManager.notifyCommittedTransactions(transactions.size());
      //mark the entries committed here after they are inserted in the commit log.
      for (TransactionEntry transactionEntry : transactionEntries) {
         transactionEntry.committed();
      }
      //then we can remove them from the transaction queue.
      sortedTransactionQueue.hasTransactionReadyToCommit();
   }

   //DEBUG ONLY!
   public final TransactionEntry getTransactionEntry(GlobalTransaction globalTransaction) {
      return sortedTransactionQueue.getTransactionEntry(globalTransaction);
   }

   public final int size() {
      return sortedTransactionQueue.size();
   }
}
