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
package org.infinispan.interceptors.gmu;

import org.infinispan.CacheException;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.GMUCommitCommand;
import org.infinispan.commands.tx.GMUPrepareCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.ApplyDeltaCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.gmu.InternalGMUCacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.container.versioning.gmu.GMUCacheEntryVersion;
import org.infinispan.container.versioning.gmu.GMUVersionGenerator;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.context.SingleKeyNonTxInvocationContext;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.EntryWrappingInterceptor;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.CacheStore;
import org.infinispan.stats.TransactionsStatisticsRegistry;
import org.infinispan.stats.translations.ExposedStatistics;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.RemoteTransaction;
import org.infinispan.transaction.gmu.CommitLog;
import org.infinispan.transaction.gmu.manager.CommittedTransaction;
import org.infinispan.transaction.gmu.manager.TransactionCommitManager;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.concurrent.BlockingTaskAwareExecutorService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import static org.infinispan.transaction.gmu.GMUHelper.*;
import static org.infinispan.transaction.gmu.manager.SortedTransactionQueue.TransactionEntry;

/**
 * @author Pedro Ruivo
 * @since 5.2
 */
public class GMUEntryWrappingInterceptor extends EntryWrappingInterceptor {

   private static final Log log = LogFactory.getLog(GMUEntryWrappingInterceptor.class);
   protected GMUVersionGenerator versionGenerator;
   private TransactionCommitManager transactionCommitManager;
   private InvocationContextContainer invocationContextContainer;
   private BlockingTaskAwareExecutorService gmuExecutor;
   private TransactionManager transactionManager;
   private CacheLoaderManager cacheLoaderManager;
   private CacheStore store;

   @Inject
   public void inject(TransactionCommitManager transactionCommitManager, DataContainer dataContainer,
                      CommitLog commitLog, VersionGenerator versionGenerator, InvocationContextContainer invocationContextContainer,
                      @ComponentName(value = KnownComponentNames.GMU_EXECUTOR) BlockingTaskAwareExecutorService gmuExecutor,
                      TransactionManager transactionManager, CacheLoaderManager cacheLoaderManager) {
      this.transactionCommitManager = transactionCommitManager;
      this.versionGenerator = toGMUVersionGenerator(versionGenerator);
      this.invocationContextContainer = invocationContextContainer;
      this.gmuExecutor = gmuExecutor;
      this.transactionManager = transactionManager;
      this.cacheLoaderManager = cacheLoaderManager;
   }

   @Start(priority = 16) //after cache store interceptor
   public void enableCacheStore() {
      if (cacheLoaderManager.isShared()) {
         throw new IllegalStateException("Shared cache store is not supported.");
      }
      store = cacheLoaderManager.isUsingPassivation() ? null : cacheLoaderManager.getCacheStore();
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      if (command.isOnePhaseCommit()) {
         throw new IllegalStateException("GMU does not support one phase commits.");
      }

      GMUPrepareCommand spc = convert(command, GMUPrepareCommand.class);

      if (ctx.isOriginLocal()) {
         spc.setVersion(ctx.getTransactionVersion());
         spc.setReadSet(ctx.getReadSet());
      } else {
         ctx.setTransactionVersion(spc.getPrepareVersion());
      }

      wrapEntriesForPrepare(ctx, command);
      performValidation(ctx, spc);

      Object retVal = invokeNextInterceptor(ctx, command);

      if (ctx.isOriginLocal() && command.getModifications().length > 0) {
         EntryVersion commitVersion = calculateCommitVersion(ctx.getTransactionVersion(), versionGenerator,
                                                             cdl.getWriteOwners(ctx.getCacheTransaction()));
         ctx.setTransactionVersion(commitVersion);
      } else {
         retVal = ctx.getTransactionVersion();
      }

      return retVal;
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      GMUCommitCommand gmuCommitCommand = convert(command, GMUCommitCommand.class);
      TransactionEntry transactionEntry = null;

      if (ctx.isOriginLocal()) {
         gmuCommitCommand.setCommitVersion(ctx.getTransactionVersion());
         transactionEntry = transactionCommitManager.commitTransaction(gmuCommitCommand.getGlobalTransaction(),
                                                                       gmuCommitCommand.getCommitVersion());
         //see org.infinispan.tx.gmu.DistConsistencyTest3.testNoCommitDeadlock
         //the commitTransaction() can re-order the queue. we need to check for pending commit commands.
         //if not, the queue can be blocked forever.
         gmuExecutor.checkForReadyTasks();
      } else {
         ctx.setTransactionVersion(gmuCommitCommand.getCommitVersion());
      }

      Object retVal = null;
      try {
         retVal = invokeNextInterceptor(ctx, command);
         //in remote context, the commit command will be enqueue, so it does not need to wait
         if (transactionEntry != null) {     //Only local
            boolean waited = TransactionsStatisticsRegistry.hasStatisticCollector() && !transactionEntry.isReadyToCommit();
            long waitTime = 0L;
            if (waited) {
               waitTime = System.nanoTime();
            }
            transactionEntry.awaitUntilIsReadyToCommit();
            if (waited) {
               TransactionsStatisticsRegistry.incrementValue(ExposedStatistics.IspnStats.NUM_WAITS_IN_COMMIT_QUEUE);
               TransactionsStatisticsRegistry.addValue(ExposedStatistics.IspnStats.WAIT_TIME_IN_COMMIT_QUEUE, System.nanoTime() - waitTime);
            }
         } else if (!ctx.isOriginLocal()) {
            transactionEntry = gmuCommitCommand.getTransactionEntry();
         }

         if (transactionEntry == null || transactionEntry.isCommitted()) {
            if (ctx.getCacheTransaction().getAllModifications().isEmpty()) {
               //this is a read-only tx... we need to store the loaded data
               for (CacheEntry entry : ctx.getLookedUpEntries().values()) {
                  if (entry.isLoaded()) {
                     commitContextEntry(entry, ctx, false);
                  }
               }
            }
            return retVal;
         }

         Iterator<TransactionEntry> toCommit = transactionCommitManager.getTransactionsToCommit().iterator();
         if (!toCommit.hasNext()) {
            throw new IllegalStateException();
         }
         TransactionEntry first = toCommit.next();
         if (!first.getGlobalTransaction().equals(transactionEntry.getGlobalTransaction())) {
            throw new IllegalStateException();
         }

         List<CommittedTransaction> committedTransactions = new ArrayList<CommittedTransaction>(4);
         List<TransactionEntry> committedTransactionEntries = new ArrayList<TransactionEntry>(4);
         int subVersion = 0;
         CacheTransaction cacheTransaction = transactionEntry.getCacheTransactionForCommit();
         CommittedTransaction committedTransaction = new CommittedTransaction(cacheTransaction, subVersion,
                                                                              transactionEntry.getConcurrentClockNumber());
         updateCommitVersion(ctx, cacheTransaction, subVersion);
         commitContextEntries(ctx, false, isFromStateTransfer(ctx));

         committedTransactions.add(committedTransaction);
         committedTransactionEntries.add(transactionEntry);

         //in case of transaction has the same version... should be rare...
         while (toCommit.hasNext()) {
            transactionEntry = toCommit.next();
            subVersion++;
            cacheTransaction = transactionEntry.getCacheTransactionForCommit();
            committedTransaction = new CommittedTransaction(cacheTransaction, subVersion,
                                                            transactionEntry.getConcurrentClockNumber());
            InvocationContext context = createInvocationContext(cacheTransaction, subVersion);
            commitContextEntries(context, false, isFromStateTransfer(ctx));
            committedTransactions.add(committedTransaction);
            committedTransactionEntries.add(transactionEntry);
         }
         for (TransactionEntry txEntry : committedTransactionEntries) {
            store(txEntry.getGlobalTransaction());
         }
         transactionCommitManager.transactionCommitted(committedTransactions, committedTransactionEntries);
      } catch (Throwable throwable) {
         //let ignore the exception. we cannot have some nodes applying the write set and another not another one
         //receives the rollback and don't applies the write set
         log.error("Error while committing transaction", throwable);
         transactionCommitManager.rollbackTransaction(ctx.getCacheTransaction());
      } finally {
         if (ctx.isOriginLocal()) {
            gmuExecutor.checkForReadyTasks();
         }
      }
      return retVal;
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      try {
         return invokeNextInterceptor(ctx, command);
      } finally {
         transactionCommitManager.rollbackTransaction(ctx.getCacheTransaction());
         if (ctx.isOriginLocal()) {
            gmuExecutor.checkForReadyTasks();
         }
      }
   }

   /*
    * NOTE: these are the only commands that passes values to the application and these keys needs to be validated
    * and added to the transaction read set.
    */

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      ctx.clearKeyReadInCommand();
      Object retVal = super.visitGetKeyValueCommand(ctx, command);
      updateTransactionVersion(ctx);
      return retVal;
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      ctx.clearKeyReadInCommand();
      Object retVal = super.visitPutKeyValueCommand(ctx, command);
      updateTransactionVersion(ctx);
      return retVal;
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      ctx.clearKeyReadInCommand();
      Object retVal = super.visitRemoveCommand(ctx, command);
      updateTransactionVersion(ctx);
      return retVal;
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      ctx.clearKeyReadInCommand();
      Object retVal = super.visitReplaceCommand(ctx, command);
      updateTransactionVersion(ctx);
      return retVal;
   }

   @Override
   protected void commitContextEntry(CacheEntry entry, InvocationContext ctx, boolean skipOwnershipCheck) {
      if (ctx.isInTxScope() && !entry.isLoaded()) {
         cdl.commitEntry(entry, ((TxInvocationContext) ctx).getTransactionVersion(), skipOwnershipCheck, ctx);
      } else {
         cdl.commitEntry(entry, entry.getVersion(), skipOwnershipCheck, ctx);
      }
   }

   /**
    * validates the read set and returns the prepare version from the commit queue
    *
    * @param ctx     the context
    * @param command the prepare command
    * @throws InterruptedException if interrupted
    */
   protected void performValidation(TxInvocationContext ctx, GMUPrepareCommand command) throws InterruptedException {
      boolean hasToUpdateLocalKeys = hasLocalKeysToUpdate(command.getModifications());
      boolean isReadOnly = command.getModifications().length == 0;

      for (Object key : command.getAffectedKeys()) {
         if (cdl.localNodeIsOwner(key)) {
            hasToUpdateLocalKeys = true;
            break;
         }
      }

      if (!hasToUpdateLocalKeys) {
         for (WriteCommand writeCommand : command.getModifications()) {
            if (writeCommand instanceof ClearCommand) {
               hasToUpdateLocalKeys = true;
               break;
            }
         }
      }

      if (!isReadOnly) {
         cdl.performReadSetValidation(ctx, command);
         if (hasToUpdateLocalKeys) {
            transactionCommitManager.prepareTransaction(ctx.getCacheTransaction());
         } else {
            transactionCommitManager.prepareReadOnlyTransaction(ctx.getCacheTransaction());
         }
      }

      if (log.isDebugEnabled()) {
         log.debugf("Transaction %s can commit on this node. Prepare Version is %s",
                    command.getGlobalTransaction().globalId(), ctx.getTransactionVersion());
      }
   }

   private void store(GlobalTransaction globalTransaction) {
      if (store == null) {
         return;
      }
      Transaction tx = safeSuspend();
      try {
         store.commit(globalTransaction);
      } catch (CacheLoaderException e) {
         //ignored
      } finally {
         safeResume(tx);
      }
   }

   private Transaction safeSuspend() {
      if (transactionManager != null) {
         try {
            return transactionManager.suspend();
         } catch (Exception e) {
            //ignored
         }
      }
      return null;
   }

   private void safeResume(Transaction tx) {
      if (transactionManager != null && tx != null) {
         try {
            transactionManager.resume(tx);
         } catch (Exception e) {
            //ignored
         }
      }
   }

   private void updateTransactionVersion(InvocationContext context) {
      if (!context.isInTxScope() && !context.isOriginLocal()) {
         return;
      }

      if (context instanceof SingleKeyNonTxInvocationContext) {
         if (log.isDebugEnabled()) {
            log.debugf("Received a SingleKeyNonTxInvocationContext... This should be a single read operation");
         }
         return;
      }

      TxInvocationContext txInvocationContext = (TxInvocationContext) context;
      List<EntryVersion> entryVersionList = new LinkedList<EntryVersion>();
      entryVersionList.add(txInvocationContext.getTransactionVersion());

      if (log.isTraceEnabled()) {
         log.tracef("[%s] Keys read in this command: %s", txInvocationContext.getGlobalTransaction().globalId(),
                    txInvocationContext.getKeysReadInCommand());
      }

      for (InternalGMUCacheEntry internalGMUCacheEntry : txInvocationContext.getKeysReadInCommand().values()) {
         if (txInvocationContext.hasModifications() && !internalGMUCacheEntry.isMostRecent()) {
            throw new CacheException("Read-Write transaction read an old value and should rollback");
         }

         if (internalGMUCacheEntry.getMaximumTransactionVersion() != null) {
            entryVersionList.add(internalGMUCacheEntry.getMaximumTransactionVersion());
         }
         txInvocationContext.getCacheTransaction().addReadKey(internalGMUCacheEntry.getKey());
         if (cdl.localNodeIsOwner(internalGMUCacheEntry.getKey())) {
            txInvocationContext.setAlreadyReadOnThisNode(true);
            txInvocationContext.addReadFrom(cdl.getAddress());
         }
      }

      if (entryVersionList.size() > 1) {
         EntryVersion[] txVersionArray = new EntryVersion[entryVersionList.size()];
         txInvocationContext.setTransactionVersion(versionGenerator.mergeAndMax(entryVersionList.toArray(txVersionArray)));
      }
   }

   private boolean hasLocalKeysToUpdate(WriteCommand[] modifications) {
      for (WriteCommand writeCommand : modifications) {
         if (writeCommand instanceof ClearCommand) {
            return true;
         } else if (writeCommand instanceof ApplyDeltaCommand) {
            if (cdl.localNodeIsOwner(((ApplyDeltaCommand) writeCommand).getKey())) {
               return true;
            }
         } else {
            for (Object key : writeCommand.getAffectedKeys()) {
               if (cdl.localNodeIsOwner(key)) {
                  return true;
               }
            }
         }
      }
      return false;
   }

   private void updateCommitVersion(TxInvocationContext context, CacheTransaction cacheTransaction, int subVersion) {
      GMUCacheEntryVersion newVersion = versionGenerator.convertVersionToWrite(cacheTransaction.getTransactionVersion(),
                                                                               subVersion);
      context.getCacheTransaction().setTransactionVersion(newVersion);
   }

   private TxInvocationContext createInvocationContext(CacheTransaction cacheTransaction, int subVersion) {
      GMUCacheEntryVersion cacheEntryVersion = versionGenerator.convertVersionToWrite(cacheTransaction.getTransactionVersion(),
                                                                                      subVersion);
      cacheTransaction.setTransactionVersion(cacheEntryVersion);
      if (cacheTransaction instanceof LocalTransaction) {
         LocalTxInvocationContext localTxInvocationContext = invocationContextContainer.createTxInvocationContext();
         localTxInvocationContext.setLocalTransaction((LocalTransaction) cacheTransaction);
         return localTxInvocationContext;
      } else if (cacheTransaction instanceof RemoteTransaction) {
         return invocationContextContainer.createRemoteTxInvocationContext((RemoteTransaction) cacheTransaction, null);
      }
      throw new IllegalStateException("Expected a remote or local transaction and not " + cacheTransaction);
   }

}
