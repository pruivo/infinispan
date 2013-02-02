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
package org.infinispan.transaction.totalorder;

import org.infinispan.CacheException;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.executors.ConditionalExecutorService;
import org.infinispan.executors.ConditionalRunnable;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.jmx.annotations.DisplayType;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.Units;
import org.infinispan.statetransfer.TransactionInfo;
import org.infinispan.transaction.RemoteTransaction;
import org.infinispan.transaction.TxDependencyLatch;
import org.infinispan.transaction.xa.GlobalTransaction;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.infinispan.factories.KnownComponentNames.CONDITIONAL_EXECUTOR;

/**
 * @author Pedro Ruivo
 * @author Mircea.markus@jboss.org
 * @since 5.2
 */
@MBean(objectName = "ParallelTotalOrderManager", description = "Concurrent total order management")
public class ParallelTotalOrderManager extends BaseTotalOrderManager {

   private final AtomicLong waitTimeInQueue = new AtomicLong(0);
   /**
    * this map is used to keep track of concurrent transactions.
    */
   private final ConcurrentMap<Object, TxDependencyLatch> keysLocked = new ConcurrentHashMap<Object, TxDependencyLatch>();
   private final AtomicReference<TxDependencyLatch> clear = new AtomicReference<TxDependencyLatch>(null);
   //pending transactions from state transfer
   private final StateTransferState stateTransferState = new StateTransferState();
   private ConditionalExecutorService validationExecutorService;
   private InvocationContextContainer invocationContextContainer;

   @Inject
   public void inject(@ComponentName(CONDITIONAL_EXECUTOR) ConditionalExecutorService executorService,
                      InvocationContextContainer invocationContextContainer) {
      this.validationExecutorService = executorService;
      this.invocationContextContainer = invocationContextContainer;
   }

   @Override
   public final void processTransactionFromSequencer(PrepareCommand prepareCommand, TxInvocationContext ctx,
                                                     CommandInterceptor invoker) throws Exception {
      logAndCheckContext(prepareCommand, ctx);
      copyLookedUpEntriesToRemoteContext(ctx);
      RemoteTransaction remoteTransaction = (RemoteTransaction) ctx.getCacheTransaction();
      Set<TxDependencyLatch> conflictingTransactions = getConflictingTransactions(remoteTransaction);
      validationExecutorService.execute(new ValidateRunnable(prepareCommand, ctx, invoker, conflictingTransactions));
   }

   @Override
   public void notifyTransactionTransferStart() {
      stateTransferState.transactionTransferStart();
   }

   @Override
   public void notifyTransactionTransferEnd() {
      stateTransferState.transactionTransferEnd();
   }

   @Override
   public final void addTransactions(Collection<TransactionInfo> transactionsInfo) {
      for (TransactionInfo transactionInfo : transactionsInfo) {
         GlobalTransaction globalTransaction = transactionInfo.getGlobalTransaction();
         RemoteTransaction remoteTransaction = transactionTable.getOrCreateIfAbsentRemoteTransaction(globalTransaction);
         remoteTransaction.updateIfNeeded(transactionInfo);
         Object[] keys = getKeysToLock(remoteTransaction.getModifications());
         stateTransferState.addTransaction(remoteTransaction, keys);
      }
   }

   @ManagedOperation(description = "Resets the statistics", displayName = "Reset Statistic")
   @Override
   public void resetStatistics() {
      super.resetStatistics();
      waitTimeInQueue.set(0);
   }

   @ManagedAttribute(description = "Average time in the queue before the validation (milliseconds)",
                     displayName = "Average Waiting Duration In Queue", units = Units.MILLISECONDS,
                     displayType = DisplayType.SUMMARY)
   public double getAverageWaitingTimeInQueue() {
      long time = waitTimeInQueue.get();
      int tx = numberOfTxValidated.get();
      if (tx == 0) {
         return 0;
      }
      return (time / tx) / 1000000.0;
   }

   @Override
   protected void releaseResources(RemoteTransaction remoteTransaction) {
      stateTransferState.transactionCompleted(remoteTransaction.getGlobalTransaction());
      TxDependencyLatch latch = remoteTransaction.getLatch();
      latch.countDown();
      Set<Object> keysModified = remoteTransaction.getLockedKeys();

      if (keysModified == null) {
         clear.compareAndSet(latch, null);
      } else {
         for (Object key : keysModified) {
            keysLocked.remove(key, latch);
         }
      }
   }

   /**
    * returns a set of transaction dependency latches where the transaction must wait until be released
    *
    * @param remoteTransaction the remote transaction
    * @return a set of transaction dependency latches
    */
   private Set<TxDependencyLatch> getConflictingTransactions(RemoteTransaction remoteTransaction) {
      TxDependencyLatch latch = remoteTransaction.getLatch();
      Object[] keysModified = getKeysToLock(remoteTransaction.getModifications());
      Set<TxDependencyLatch> previousConflictingTransactions = new HashSet<TxDependencyLatch>();

      if (keysModified == null) { //clear command
         TxDependencyLatch oldClear = clear.get();
         if (oldClear != null) {
            previousConflictingTransactions.add(oldClear);
            clear.set(latch);
         }
         //add all other "locks"
         previousConflictingTransactions.addAll(keysLocked.values());
         keysLocked.clear();
      } else {
         TxDependencyLatch clearTx = clear.get();
         //this will collect all the count down latch corresponding to the previous transactions in the queue
         for (Object key : keysModified) {
            TxDependencyLatch prevTx = keysLocked.put(key, remoteTransaction.getLatch());
            if (prevTx != null) {
               previousConflictingTransactions.add(prevTx);
            } else if (clearTx != null) {
               previousConflictingTransactions.add(clearTx);
            }
            remoteTransaction.registerLockedKey(key);
         }
      }

      previousConflictingTransactions.addAll(stateTransferState.getDependencyLatches(keysModified));

      if (trace) {
         log.tracef("Transaction [%s] write set is %s. Conflicting transactions are %s",
                    remoteTransaction.getGlobalTransaction().prettyPrint(), keysModified, previousConflictingTransactions);
      }

      return previousConflictingTransactions;
   }

   //================== JMX ================

   /**
    * updates the accumulating time for profiling information
    *
    * @param creationTime     the arrival timestamp of the prepare command to this component in remote
    * @param processStartTime the processing start timestamp
    * @param processEndTime   the processing ending timestamp
    */
   private void updateDurationStats(long creationTime, long processStartTime, long processEndTime) {
      if (statisticsEnabled) {
         //set the profiling information
         waitTimeInQueue.addAndGet(processStartTime - creationTime);
         processingDuration.addAndGet(processEndTime - processStartTime);
         numberOfTxValidated.incrementAndGet();
      }
   }

   /**
    * This class is used to validate transaction in repeatable read with write skew check
    */
   protected class ValidateRunnable implements ConditionalRunnable {

      protected final RemoteTransaction remoteTransaction;
      protected final PrepareCommand prepareCommand;
      //the set of others transaction's count down latch (it will be unblocked when the transaction finishes)
      private final Set<TxDependencyLatch> conflictingTransactions;
      private final TxInvocationContext txInvocationContext;
      private final CommandInterceptor invoker;
      private final long creationTime;

      protected ValidateRunnable(PrepareCommand prepareCommand, TxInvocationContext txInvocationContext,
                                 CommandInterceptor invoker, Set<TxDependencyLatch> conflictingTransactions) {
         if (prepareCommand == null || txInvocationContext == null || invoker == null) {
            throw new IllegalArgumentException("Arguments must not be null");
         }
         this.prepareCommand = prepareCommand;
         this.txInvocationContext = txInvocationContext;
         this.invoker = invoker;
         this.creationTime = now();
         this.conflictingTransactions = conflictingTransactions;
         this.remoteTransaction = (RemoteTransaction) txInvocationContext.getCacheTransaction();
         //ensure not loops
         this.conflictingTransactions.remove(remoteTransaction.getLatch());

      }

      @Override
      public boolean isReady() {
         return isAllZero(conflictingTransactions) && !isIncomingStateTransfer(remoteTransaction.getModifications());
      }

      @Override
      public void run() {
         long processStartTime = now();
         Object result = null;
         boolean exceptionThrown = false;
         try {
            if (trace) log.tracef("Validating transaction %s ",
                                  prepareCommand.getGlobalTransaction().prettyPrint());

            initializeValidation();

            //invoke next interceptor in the chain
            result = prepareCommand.acceptVisitor(txInvocationContext, invoker);
         } catch (Throwable throwable) {
            log.trace("Exception while processing the rest of the interceptor chain", throwable);
            result = throwable;
            exceptionThrown = true;
         } finally {
            long processEndTime = now();
            if (result instanceof EntryVersionsMap) {
               prepareCommand.sendReply(new HashSet<Object>(((EntryVersionsMap) result).keySet()), false);
            }
            {
               prepareCommand.sendReply(result, exceptionThrown);
            }

            remoteTransaction.prepared();

            if (prepareCommand.isOnePhaseCommit()) {
               finishTransaction(remoteTransaction, !exceptionThrown);
            } else if (exceptionThrown) {
               releaseResources(remoteTransaction);
            }

            updateDurationStats(creationTime, processStartTime, processEndTime);
         }
      }

      /**
       * set the initialization of the thread before the validation
       */
      private void initializeValidation() {
         String gtx = prepareCommand.getGlobalTransaction().prettyPrint();
         //TODO is this really needed?
         invocationContextContainer.setContext(txInvocationContext);

         remoteTransaction.preparing();

         if (remoteTransaction.isRollbackReceived()) {
            //this means that rollback has already been received
            transactionTable.removeRemoteTransaction(remoteTransaction.getGlobalTransaction());
            throw new CacheException("Cannot prepare transaction" + gtx + ". it was already marked as rollback");
         }

         if (remoteTransaction.isCommitReceived()) {
            log.tracef("Transaction %s marked for commit, skipping the write skew check and forcing 1PC", gtx);
            if (prepareCommand instanceof VersionedPrepareCommand) {
               ((VersionedPrepareCommand) prepareCommand).setSkipWriteSkewCheck(true);
            }
            prepareCommand.setOnePhaseCommit(true);
         }
      }
   }
}
