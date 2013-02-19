/*
 * INESC-ID, Instituto de Engenharia de Sistemas e Computadores Investigação e Desevolvimento em Lisboa
 * Copyright 2013 INESC-ID and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 3.0 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.transaction.gmu.manager;

import org.infinispan.Cache;
import org.infinispan.commands.tx.GMUCommitCommand;
import org.infinispan.container.CommitContextEntries;
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
   private final SortedTransactionQueue sortedTransactionQueue;
   private long lastPreparedVersion = 0;
   private CommitThread commitThread;
   private InvocationContextContainer icc;
   private GMUVersionGenerator versionGenerator;
   private CommitLog commitLog;
   private Transport transport;
   private Cache cache;
   private CommitContextEntries commitContextEntries;
   private GarbageCollectorManager garbageCollectorManager;

   public TransactionCommitManager() {
      sortedTransactionQueue = new SortedTransactionQueue();
   }

   @Inject
   public void inject(InvocationContextContainer icc, VersionGenerator versionGenerator, CommitLog commitLog,
                      Transport transport, Cache cache, CommitContextEntries commitContextEntries,
                      GarbageCollectorManager garbageCollectorManager) {
      this.icc = icc;
      this.versionGenerator = toGMUVersionGenerator(versionGenerator);
      this.commitLog = commitLog;
      this.transport = transport;
      this.cache = cache;
      this.commitContextEntries = commitContextEntries;
      this.garbageCollectorManager = garbageCollectorManager;
   }

   //AFTER THE VersionGenerator
   @Start(priority = 31)
   public void start() {
      commitThread = new CommitThread(transport.getAddress() + "-" + cache.getName() + "-GMU-Commit");
      commitThread.start();
   }

   @Stop
   public void stop() {
      commitThread.interrupt();
      //TODO
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

                     CommittedTransaction committedTransaction = new CommittedTransaction(cacheTransaction, subVersion, transactionEntry.getConcurrentClockNumber());

                     commitContextEntries.commitContextEntries(createInvocationContext(cacheTransaction, subVersion));
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
