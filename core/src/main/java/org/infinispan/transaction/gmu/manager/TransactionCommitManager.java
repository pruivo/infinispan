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
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.container.versioning.gmu.GMUCacheEntryVersion;
import org.infinispan.container.versioning.gmu.GMUVersion;
import org.infinispan.container.versioning.gmu.GMUVersionGenerator;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.gmu.GMUEntryWrappingInterceptor;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.transaction.gmu.CommitLog;
import org.infinispan.transaction.gmu.GMUHelper;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

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
         this.versionGenerator = (GMUVersionGenerator) versionGenerator;
      }
      this.commitLog = commitLog;
      this.garbageCollectorManager = garbageCollectorManager;
   }

   /**
    * add a transaction to the queue. A temporary commit vector clock is associated and with it, it order the
    * transactions
    *
    * @param cacheTransaction  the transaction to be prepared
    * @param fromStateTransfer {@code true} if the transaction is from state transfer.
    */
   public synchronized void prepareTransaction(CacheTransaction cacheTransaction, boolean fromStateTransfer) {
      long concurrentClockNumber = commitLog.getCurrentVersion().getThisNodeVersionValue();
      EntryVersion txVersion = !fromStateTransfer ? commitLog.getCurrentVersion() :
            versionGenerator.mergeAndMax(commitLog.getCurrentVersion(), cacheTransaction.getTransactionVersion());
      EntryVersion preparedVersion = versionGenerator.setNodeVersion(txVersion,
                                                                     ++lastPreparedVersion);

      cacheTransaction.setTransactionVersion(preparedVersion);
      sortedTransactionQueue.prepare(cacheTransaction, concurrentClockNumber);
   }

   /**
    * add a sender state transfer (shadow) transaction to the queue.
    *
    * @param other The other node for this state transfer
    */
   public synchronized EntryVersion prepareSenderStateTransferTransaction(Address other) {
      long concurrentClockNumber = commitLog.getCurrentVersion().getThisNodeVersionValue();
      EntryVersion txVersion = commitLog.getCurrentVersion();
      EntryVersion preparedVersion = versionGenerator.setNodeVersion(txVersion, ++lastPreparedVersion);
      sortedTransactionQueue.prepareSenderOnStateTransfer(other, preparedVersion, concurrentClockNumber);
      return preparedVersion;
   }

   /**
    * add a receiver state transfer (shadow) transaction to the queue.
    *
    * @param other The other node for this state transfer
    */
   public synchronized EntryVersion prepareReceiverStateTransferTransaction(Address other) {
      long concurrentClockNumber = commitLog.getCurrentVersion().getThisNodeVersionValue();
      EntryVersion txVersion = commitLog.getCurrentVersion();
      EntryVersion preparedVersion = versionGenerator.setNodeVersion(txVersion, ++lastPreparedVersion);
      sortedTransactionQueue.prepareReceiverOnStateTransfer(other, preparedVersion, concurrentClockNumber);
      return preparedVersion;
   }

   public void rollbackTransaction(CacheTransaction cacheTransaction) {
      sortedTransactionQueue.rollback(cacheTransaction);
   }

   public void rollbackReceiverStateTransferTransaction(Address other) {
      sortedTransactionQueue.rollbackReceiverOnStateTransfer(other);
   }

   public void rollbackSenderStateTransferTransaction(Address other) {
      sortedTransactionQueue.rollbackSenderOnStateTransfer(other);
   }

   public synchronized TransactionEntry commitTransaction(GlobalTransaction globalTransaction, EntryVersion version) {
      final GMUVersion commitVersion = (GMUVersion) version;
      lastPreparedVersion = Math.max(commitVersion.getThisNodeVersionValue(), lastPreparedVersion);
      TransactionEntry entry = sortedTransactionQueue.commit(globalTransaction, commitVersion);
      if (entry == null) {
         commitLog.updateMostRecentVersion(commitVersion);
      }
      return entry;
   }

   public synchronized TransactionEntry commitReceiverStateTransferTransaction(Address other, EntryVersion version) {
      final GMUVersion commitVersion = (GMUVersion) version;
      lastPreparedVersion = Math.max(commitVersion.getThisNodeVersionValue(), lastPreparedVersion);
      TransactionEntry entry = sortedTransactionQueue.commitReceiverOnStateTransfer(other, commitVersion);
      if (entry == null) {
         commitLog.updateMostRecentVersion(commitVersion);
      }
      return entry;
   }

   public synchronized TransactionEntry commitSenderStateTransferTransaction(Address other, EntryVersion version) {
      final GMUVersion commitVersion = (GMUVersion) version;
      lastPreparedVersion = Math.max(commitVersion.getThisNodeVersionValue(), lastPreparedVersion);
      TransactionEntry entry = sortedTransactionQueue.commitSenderOnStateTransfer(other, commitVersion);
      if (entry == null) {
         commitLog.updateMostRecentVersion(commitVersion);
      }
      return entry;
   }

   public EntryVersion computeFinalCommitVersionOnStateTransfer(EntryVersion preparedVersion, EntryVersion proposal, Address sender, Address destination){

      EntryVersion[] versions = new EntryVersion[2];
      versions[0] = preparedVersion;
      versions[1] = proposal;

      EntryVersion merged = GMUHelper.joinVersions(versions, versionGenerator);
      List<Address> participants = new LinkedList<Address>();
      participants.add(sender);
      participants.add(destination);
      return GMUHelper.calculateCommitVersion(merged, versionGenerator, participants);
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
      sortedTransactionQueue.notifyTransactionsCommitted();
   }

   public void executeCommit(SortedTransactionQueue.TransactionEntry currentEntry, Collection<SortedTransactionQueue.TransactionEntry> transactionsToCommit, TxInvocationContext ctx, GMUEntryWrappingInterceptor GMUInterceptor) throws InterruptedException {

      if (!transactionsToCommit.isEmpty()) {

         List<CommittedTransaction> committedTransactions = new ArrayList<CommittedTransaction>(transactionsToCommit.size());
         List<SortedTransactionQueue.TransactionEntry> committedTransactionEntries = new ArrayList<SortedTransactionQueue.TransactionEntry>(transactionsToCommit.size());
         int subVersion = 0;

         //in case of transaction has the same version... should be rare...
         for (SortedTransactionQueue.TransactionEntry toCommit : transactionsToCommit) {
            if (!toCommit.committing()) {
               toCommit.awaitUntilCommitted();
               continue;
            }
            CacheTransaction cacheTransaction = toCommit.getCacheTransactionForCommit();
            CommittedTransaction committedTransaction = null;
            if(cacheTransaction == null){
               //This entry is not associated to a CacheTransaction. This one is associated to a shadow transaction.
               committedTransaction = new CommittedTransaction(toCommit.getVersion(), subVersion, toCommit.getConcurrentClockNumber());
            }
            else{
               committedTransaction = new CommittedTransaction(cacheTransaction, subVersion, toCommit.getConcurrentClockNumber());
            }

            TxInvocationContext context = null;

            if(cacheTransaction != null){
               if (ctx != null && currentEntry.getGlobalTransaction() != null && currentEntry.getGlobalTransaction().equals(toCommit.getGlobalTransaction())) {
                  GMUInterceptor.updateCommitVersion(ctx, cacheTransaction, subVersion);
                  context = ctx;
               } else {
                  context = GMUInterceptor.createInvocationContext(cacheTransaction, subVersion);
               }

               toCommit.setNewVersionInDataContainer((GMUCacheEntryVersion) context.getCacheTransaction().getTransactionVersion());

               GMUInterceptor.commitContextEntries(context, false, false);
            }
            else{
               toCommit.setNewVersionInDataContainer(GMUInterceptor.inferCommitVersion(toCommit.getVersion(), subVersion));
            }



            committedTransactions.add(committedTransaction);
            committedTransactionEntries.add(toCommit);
            GMUInterceptor.updateWaitingTime(toCommit);
            subVersion++;
         }
         for (SortedTransactionQueue.TransactionEntry txEntry : committedTransactionEntries) {
            if(txEntry.getGlobalTransaction() != null)
               GMUInterceptor.store(txEntry.getGlobalTransaction());
         }
         transactionCommitted(committedTransactions, committedTransactionEntries);


      }


   }


   //DEBUG ONLY!
   public final TransactionEntry getTransactionEntry(GlobalTransaction globalTransaction) {
      return sortedTransactionQueue.getTransactionEntry(globalTransaction);
   }

   public final int size() {
      return sortedTransactionQueue.size();
   }

   public final List<String> printQueue() {
      return sortedTransactionQueue.printQueue();
   }
}
