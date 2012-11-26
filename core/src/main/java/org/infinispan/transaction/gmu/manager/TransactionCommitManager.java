package org.infinispan.transaction.gmu.manager;

import org.infinispan.Cache;
import org.infinispan.commands.tx.GMUCommitCommand;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.container.versioning.gmu.GMUCacheEntryVersion;
import org.infinispan.container.versioning.gmu.GMUVersion;
import org.infinispan.container.versioning.gmu.GMUVersionGenerator;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.interceptors.EntryWrappingInterceptor;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.RemoteTransaction;
import org.infinispan.transaction.gmu.CommitLog;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.LinkedList;
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

   private final static Log log = LogFactory.getLog(TransactionCommitManager.class);

   private long lastPreparedVersion = 0;
   private CommitThread commitThread;
   private final SortedTransactionQueue sortedTransactionQueue;
   private CommitInstance commitInvocationInstance;
   private InterceptorChain ic;
   private InvocationContextContainer icc;
   private GMUVersionGenerator versionGenerator;
   private CommitLog commitLog;
   private Transport transport;
   private Cache cache;
   private GarbageCollectorManager garbageCollectorManager;

   public TransactionCommitManager() {
      sortedTransactionQueue = new SortedTransactionQueue();
   }

   @Inject
   public void inject(InvocationContextContainer icc, VersionGenerator versionGenerator, CommitLog commitLog,
                      Transport transport, Cache cache, GarbageCollectorManager garbageCollectorManager) {
      this.icc = icc;
      this.versionGenerator = toGMUVersionGenerator(versionGenerator);
      this.commitLog = commitLog;
      this.transport = transport;
      this.cache = cache;
      this.garbageCollectorManager = garbageCollectorManager;
   }

   //AFTER THE VersionGenerator
   @Start(priority = 31)
   public void start() {
      commitThread = new CommitThread(transport.getAddress() + "-" + cache.getName() + "-GMU-Commit");
      commitThread.start();

      if(commitInvocationInstance == null) {
         List<CommandInterceptor> all = ic.getInterceptorsWhichExtend(EntryWrappingInterceptor.class);
         if(log.isDebugEnabled()) {
            log.debugf("Starting Commit Queue Component. Searching interceptors with interface CommitInstance. " +
                             "Found: %s", all);
         }
         for(CommandInterceptor ci : all) {
            if(ci instanceof CommitInstance) {
               if(log.isDebugEnabled()) {
                  log.debugf("Interceptor implementing CommitInstance found! It is %s", ci);
               }
               commitInvocationInstance = (CommitInstance) ci;
               break;
            }
         }
      }
      if(commitInvocationInstance == null) {
         throw new NullPointerException("Commit Invocation Instance must not be null in serializable mode.");
      }
   }

   @Stop
   public void stop() {
      commitThread.interrupt();
      //TODO
   }

   /**
    * add a transaction to the queue. A temporary commit vector clock is associated
    * and with it, it order the transactions
    *
    * @param cacheTransaction the transaction to be prepared
    */
   public synchronized void prepareTransaction(CacheTransaction cacheTransaction) {
      EntryVersion preparedVersion = versionGenerator.setNodeVersion(commitLog.getCurrentVersion(),
                                                                     ++lastPreparedVersion);

      cacheTransaction.setTransactionVersion(preparedVersion);
      sortedTransactionQueue.prepare(cacheTransaction);
   }

   public void rollbackTransaction(CacheTransaction cacheTransaction) {
      sortedTransactionQueue.rollback(cacheTransaction);
   }

   public synchronized void commitTransaction(CacheTransaction cacheTransaction, EntryVersion version) {
      GMUVersion commitVersion = toGMUVersion(version);
      lastPreparedVersion = Math.max(commitVersion.getThisNodeVersionValue(), lastPreparedVersion);
      if (!sortedTransactionQueue.commit(cacheTransaction, commitVersion)) {
         commitLog.updateMostRecentVersion(commitVersion);
      }
   }

   public void prepareReadOnlyTransaction(CacheTransaction cacheTransaction) {
      EntryVersion preparedVersion = commitLog.getCurrentVersion();
      cacheTransaction.setTransactionVersion(preparedVersion);
   }

   public void awaitUntilCommitted(CacheTransaction transaction, GMUCommitCommand commitCommand) throws InterruptedException {
      TransactionEntry transactionEntry = sortedTransactionQueue.getTransactionEntry(transaction.getGlobalTransaction());
      if (transactionEntry == null) {
         if (commitCommand != null) {
            commitCommand.sendReply(null, false);
         }
         return;
      }
      transactionEntry.awaitUntilCommitted(commitCommand);
   }

   public static interface CommitInstance {
      void commitTransaction(TxInvocationContext ctx);
   }

   //DEBUG ONLY!
   public final TransactionEntry getTransactionEntry(GlobalTransaction globalTransaction) {
      return sortedTransactionQueue.getTransactionEntry(globalTransaction);
   }

   private class CommitThread extends Thread {
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

                     committedTransactions.add(committedTransaction);

                     if (log.isTraceEnabled()) {
                        log.tracef("Transaction entries committed for %s", transactionEntry);
                     }
                  } catch (Exception e) {
                     log.warnf("Error occurs while committing transaction entries for %s", transactionEntry);
                  } finally {
                     icc.clearThreadLocal();
                     subVersion++;
                     garbageCollectorManager.notifyCommittedTransaction();
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
   }
}
