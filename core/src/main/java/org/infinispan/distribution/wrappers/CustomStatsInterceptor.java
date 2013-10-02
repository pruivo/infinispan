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
package org.infinispan.distribution.wrappers;

import org.infinispan.commands.SetTransactionClassCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.GMUPrepareCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.base.BaseCustomInterceptor;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.Parameter;
import org.infinispan.remoting.InboundInvocationHandler;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.CommandAwareRpcDispatcher;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.stats.ExposedStatistic;
import org.infinispan.stats.LockRelatedStatsHelper;
import org.infinispan.stats.TransactionsStatisticsRegistry;
import org.infinispan.stats.container.TransactionStatistics;
import org.infinispan.stats.topK.StreamLibContainer;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.WriteSkewException;
import org.infinispan.transaction.gmu.ValidationException;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.locks.DeadlockDetectedException;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.lang.reflect.Field;

import static org.infinispan.stats.ExposedStatistic.*;

/**
 * Massive hack for a noble cause!
 *
 * @author Mircea Markus <mircea.markus@jboss.com> (C) 2011 Red Hat Inc.
 * @author Diego Didona <didona@gsd.inesc-id.pt>
 * @author Pedro Ruivo
 * @since 5.2
 */
@MBean(objectName = "ExtendedStatistics",
       description = "Component that manages and exposes extended statistics " +
             "relevant to transactions.")
public final class CustomStatsInterceptor extends BaseCustomInterceptor {
   //TODO what about the transaction implicit vs transaction explicit? should we take in account this and ignore
   //the implicit stuff?

   private final Log log = LogFactory.getLog(getClass());
   private TransactionTable transactionTable;
   private Configuration configuration;
   private RpcManager rpcManager;
   private DistributionManager distributionManager;

   @Inject
   public void inject(TransactionTable transactionTable, Configuration config, DistributionManager distributionManager) {
      this.transactionTable = transactionTable;
      this.configuration = config;
      this.distributionManager = distributionManager;
   }

   @Start(priority = 99)
   public void start() {
      // we want that this method is the last to be invoked, otherwise the start method is not invoked
      // in the real components
      replace();
      log.info("Initializing the TransactionStatisticsRegistry");
      TransactionsStatisticsRegistry.init(this.configuration);
      if (configuration.versioning().scheme().equals(VersioningScheme.GMU))
         LockRelatedStatsHelper.enable();
   }

   @Override
   public Object visitSetTransactionClassCommand(InvocationContext ctx, SetTransactionClassCommand command) throws Throwable {
      final String transactionClass = command.getTransactionalClass();

      invokeNextInterceptor(ctx, command);
      if (ctx.isInTxScope() && transactionClass != null && !transactionClass.isEmpty()) {
         final GlobalTransaction globalTransaction = ((TxInvocationContext) ctx).getGlobalTransaction();
         if (log.isTraceEnabled()) {
            log.tracef("Setting the transaction class %s for %s", transactionClass, globalTransaction);
         }
         final TransactionStatistics txs = initStatsIfNecessary(ctx);
         TransactionsStatisticsRegistry.setTransactionalClass(transactionClass, txs);
         globalTransaction.setTransactionClass(command.getTransactionalClass());
         return Boolean.TRUE;
      } else if (log.isTraceEnabled()) {
         log.tracef("Did not set transaction class %s. It is not inside a transaction or the transaction class is null",
                    transactionClass);
      }
      return Boolean.FALSE;
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      if (log.isTraceEnabled()) {
         log.tracef("Visit Put Key Value command %s. Is it in transaction scope? %s. Is it local? %s", command,
                    ctx.isInTxScope(), ctx.isOriginLocal());
      }
      Object ret;
      if (TransactionsStatisticsRegistry.isActive() && ctx.isInTxScope()) {
         final TransactionStatistics transactionStatistics = initStatsIfNecessary(ctx);
         transactionStatistics.setUpdateTransaction();
         long initTime = System.nanoTime();
         transactionStatistics.incrementValue(NUM_PUT);
         transactionStatistics.addNTBCValue(initTime);
         try {
            ret = invokeNextInterceptor(ctx, command);
         } catch (TimeoutException e) {
            if (ctx.isOriginLocal()) {
               transactionStatistics.incrementValue(NUM_LOCK_FAILED_TIMEOUT);
            }
            throw e;
         } catch (DeadlockDetectedException e) {
            if (ctx.isOriginLocal()) {
               transactionStatistics.incrementValue(NUM_LOCK_FAILED_DEADLOCK);
            }
            throw e;
         } catch (WriteSkewException e) {
            if (ctx.isOriginLocal()) {
               transactionStatistics.incrementValue(NUM_WRITE_SKEW);
            }
            throw e;
         }
         if (isRemote(command.getKey())) {
            transactionStatistics.addValue(REMOTE_PUT_EXECUTION, System.nanoTime() - initTime);
            transactionStatistics.incrementValue(NUM_REMOTE_PUT);
         }
         transactionStatistics.setLastOpTimestamp(System.nanoTime());
         return ret;
      } else
         return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      if (log.isTraceEnabled()) {
         log.tracef("Visit Get Key Value command %s. Is it in transaction scope? %s. Is it local? %s", command,
                    ctx.isInTxScope(), ctx.isOriginLocal());
      }
      Object ret;
      if (TransactionsStatisticsRegistry.isActive() && ctx.isInTxScope()) {

         final TransactionStatistics transactionStatistics = initStatsIfNecessary(ctx);
         transactionStatistics.notifyRead();
         long currCpuTime = 0;
         boolean isRemoteKey = isRemote(command.getKey());
         if (isRemoteKey) {
            currCpuTime = TransactionsStatisticsRegistry.getThreadCPUTime();
         }
         long currTimeForAllGetCommand = System.nanoTime();
         transactionStatistics.addNTBCValue(currTimeForAllGetCommand);
         ret = invokeNextInterceptor(ctx, command);
         long lastTimeOp = System.nanoTime();
         if (isRemoteKey) {
            transactionStatistics.incrementValue(NUM_LOCAL_REMOTE_GET);
            transactionStatistics.addValue(LOCAL_REMOTE_GET_R, lastTimeOp - currTimeForAllGetCommand);
            if (TransactionsStatisticsRegistry.isSampleServiceTime())
               transactionStatistics.addValue(LOCAL_REMOTE_GET_S, TransactionsStatisticsRegistry.getThreadCPUTime() - currCpuTime);
         }
         transactionStatistics.addValue(ALL_GET_EXECUTION, lastTimeOp - currTimeForAllGetCommand);
         transactionStatistics.incrementValue(NUM_GET);
         transactionStatistics.setLastOpTimestamp(lastTimeOp);
      } else {
         ret = invokeNextInterceptor(ctx, command);
      }
      return ret;
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      if (log.isTraceEnabled()) {
         log.tracef("Visit Commit command %s. Is it local?. Transaction is %s", command,
                    ctx.isOriginLocal(), command.getGlobalTransaction().globalId());
      }
      if (!TransactionsStatisticsRegistry.isActive()) {
         return invokeNextInterceptor(ctx, command);
      }

      TransactionStatistics transactionStatistics = initStatsIfNecessary(ctx);
      long currCpuTime = TransactionsStatisticsRegistry.getThreadCPUTime();
      long currTime = System.nanoTime();
      transactionStatistics.addNTBCValue(currTime);
      transactionStatistics.attachId(ctx.getGlobalTransaction());
      if (LockRelatedStatsHelper.shouldAppendLocks(configuration, true, !ctx.isOriginLocal())) {
         if (log.isTraceEnabled())
            log.trace("Appending locks for " + ((!ctx.isOriginLocal()) ? "remote " : "local ") + "transaction " +
                            ctx.getGlobalTransaction().getId());
         TransactionsStatisticsRegistry.appendLocks();
      } else {
         if (log.isTraceEnabled())
            log.trace("Not appending locks for " + ((!ctx.isOriginLocal()) ? "remote " : "local ") + "transaction " +
                            ctx.getGlobalTransaction().getId());
      }
      Object ret = invokeNextInterceptor(ctx, command);

      handleCommitCommand(transactionStatistics, currTime, currCpuTime, ctx);

      transactionStatistics.setTransactionOutcome(true);
      //We only terminate a local transaction, since RemoteTransactions are terminated from the InboundInvocationHandler
      if (ctx.isOriginLocal()) {
         TransactionsStatisticsRegistry.terminateTransaction(transactionStatistics);
      }

      return ret;
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      if (log.isTraceEnabled()) {
         log.tracef("Visit Prepare command %s. Is it local?. Transaction is %s", command,
                    ctx.isOriginLocal(), command.getGlobalTransaction().globalId());
      }

      if (!TransactionsStatisticsRegistry.isActive()) {
         return invokeNextInterceptor(ctx, command);
      }

      TransactionStatistics transactionStatistics = initStatsIfNecessary(ctx);
      transactionStatistics.onPrepareCommand();
      if (command.hasModifications()) {
         transactionStatistics.setUpdateTransaction();
      }


      boolean success = false;
      try {
         long currTime = System.nanoTime();
         long currCpuTime = TransactionsStatisticsRegistry.getThreadCPUTime();
         Object ret = invokeNextInterceptor(ctx, command);
         success = true;
         handlePrepareCommand(transactionStatistics, currTime, currCpuTime, ctx, command);
         return ret;
      } catch (TimeoutException e) {
         if (ctx.isOriginLocal()) {
            transactionStatistics.incrementValue(NUM_LOCK_FAILED_TIMEOUT);
         }
         throw e;
      } catch (DeadlockDetectedException e) {
         if (ctx.isOriginLocal()) {
            transactionStatistics.incrementValue(NUM_LOCK_FAILED_DEADLOCK);
         }
         throw e;
      } catch (WriteSkewException e) {
         if (ctx.isOriginLocal()) {
            transactionStatistics.incrementValue(NUM_WRITE_SKEW);
         }
         throw e;
      } catch (ValidationException e) {
         if (ctx.isOriginLocal()) {
            transactionStatistics.incrementValue(NUM_ABORTED_TX_DUE_TO_VALIDATION);
         }
         //Increment the killed xact only if the murder has happened on this node (whether the xact is local or remote)
         transactionStatistics.incrementValue(NUM_KILLED_TX_DUE_TO_VALIDATION);
         throw e;

      }
      //We don't care about cacheException for earlyAbort

      //Remote exception we care about
      catch (Exception e) {
         if (ctx.isOriginLocal()) {
            if (e.getCause() instanceof TimeoutException) {
               transactionStatistics.incrementValue(NUM_LOCK_FAILED_TIMEOUT);
            } else if (e.getCause() instanceof ValidationException) {
               transactionStatistics.incrementValue(NUM_ABORTED_TX_DUE_TO_VALIDATION);

            }
         }

         throw e;
      } finally {
         if (command.isOnePhaseCommit()) {
            transactionStatistics.setTransactionOutcome(success);
            if (ctx.isOriginLocal()) {
               transactionStatistics.terminateTransaction();
            }
         }
      }
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      if (log.isTraceEnabled()) {
         log.tracef("Visit Rollback command %s. Is it local?. Transaction is %s", command,
                    ctx.isOriginLocal(), command.getGlobalTransaction().globalId());
      }

      if (!TransactionsStatisticsRegistry.isActive()) {
         return invokeNextInterceptor(ctx, command);
      }

      TransactionStatistics transactionStatistics = initStatsIfNecessary(ctx);
      long currentCpuTime = TransactionsStatisticsRegistry.getThreadCPUTime();
      long initRollbackTime = System.nanoTime();
      Object ret = invokeNextInterceptor(ctx, command);
      transactionStatistics.setTransactionOutcome(false);

      handleRollbackCommand(transactionStatistics, initRollbackTime, currentCpuTime, ctx);
      if (ctx.isOriginLocal()) {
         TransactionsStatisticsRegistry.terminateTransaction(transactionStatistics);
      }

      return ret;
   }

   @ManagedAttribute(description = "Average number of puts performed by a successful local transaction",
                     displayName = "No. of puts per successful local transaction")
   public double getAvgNumPutsBySuccessfulLocalTx() {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(PUTS_PER_LOCAL_TX, null));
   }

   @ManagedAttribute(description = "Average Prepare Round-Trip Time duration (in microseconds)",
                     displayName = "Avg Prepare RTT")
   public long getAvgPrepareRtt() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((RTT_PREPARE), null)));
   }

   @ManagedAttribute(description = "Average Commit Round-Trip Time duration (in microseconds)",
                     displayName = "Avg Commit RTT")
   public long getAvgCommitRtt() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute(RTT_COMMIT, null)));
   }

   @ManagedAttribute(description = "Average Remote Get Round-Trip Time duration (in microseconds)",
                     displayName = "Avg Remote Get RTT")
   public long getAvgRemoteGetRtt() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((RTT_GET), null)));
   }

   @ManagedAttribute(description = "Average Rollback Round-Trip Time duration (in microseconds)",
                     displayName = "Avg Rollback RTT")
   public long getAvgRollbackRtt() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((RTT_ROLLBACK), null)));
   }

   @ManagedAttribute(description = "Average asynchronous Prepare duration (in microseconds)",
                     displayName = "Avg Prepare Async")
   public long getAvgPrepareAsync() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((ASYNC_PREPARE), null)));
   }

   @ManagedAttribute(description = "Average asynchronous Commit duration (in microseconds)",
                     displayName = "Avg Commit Async")
   public long getAvgCommitAsync() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute(ASYNC_COMMIT, null)));
   }

   @ManagedAttribute(description = "Average asynchronous Complete Notification duration (in microseconds)",
                     displayName = "Avg Complete Notification Async")
   public long getAvgCompleteNotificationAsync() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute(ASYNC_COMPLETE_NOTIFY, null)));
   }

   @ManagedAttribute(description = "Average asynchronous Rollback duration (in microseconds)",
                     displayName = "Avg Rollback Async")
   public long getAvgRollbackAsync() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute(ASYNC_ROLLBACK, null)));
   }

   @ManagedAttribute(description = "Average number of nodes in Commit destination set",
                     displayName = "Avg no. of Nodes in Commit Destination Set")
   public long getAvgNumNodesCommit() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute(NUM_NODES_COMMIT, null)));
   }

   @ManagedAttribute(description = "Average number of nodes of async commit messages sent per tx",
                     displayName = "Avg no. of nodes of async commit messages sent per tx")
   public long getAvgNumAsyncSentCommitMsgs() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute(SENT_ASYNC_COMMIT, null)));
   }

   @ManagedAttribute(description = "Average number of nodes of Sync commit messages sent per tx",
                     displayName = "Avg no. of nodes of Sync commit messages sent per tx")
   public long getAvgNumSyncSentCommitMsgs() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute(SENT_SYNC_COMMIT, null)));
   }

   @ManagedAttribute(description = "Average number of nodes in Complete Notification destination set",
                     displayName = "Avg no. of Nodes in Complete Notification Destination Set")
   public long getAvgNumNodesCompleteNotification() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute(NUM_NODES_COMPLETE_NOTIFY, null)));
   }

   @ManagedAttribute(description = "Average no. of nodes in Remote Get destination set",
                     displayName = "Avg no. of Nodes in Remote Get Destination Set")
   public long getAvgNumNodesRemoteGet() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute(NUM_NODES_GET, null)));
   }

   @ManagedAttribute(description = "Average number of nodes in Prepare destination set",
                     displayName = "Avg no. of Nodes in Prepare Destination Set")
   public long getAvgNumNodesPrepare() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute(NUM_NODES_PREPARE, null)));
   }

   @ManagedAttribute(description = "Average number of nodes in Rollback destination set",
                     displayName = "Avg no. of Nodes in Rollback Destination Set")
   public long getAvgNumNodesRollback() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((NUM_NODES_ROLLBACK), null)));
   }

   @ManagedAttribute(description = "Application Contention Factor",
                     displayName = "Application Contention Factor")
   public double getApplicationContentionFactor() {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(APPLICATION_CONTENTION_FACTOR, null));
   }

   @Deprecated
   @ManagedAttribute(description = "Local Contention Probability",
                     displayName = "Local Conflict Probability")
   public double getLocalContentionProbability() {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute((LOCAL_CONTENTION_PROBABILITY), null));
   }

   @Deprecated
   @ManagedAttribute(description = "Remote Contention Probability",
                     displayName = "Remote Conflict Probability")
   public double getRemoteContentionProbability() {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute((REMOTE_CONTENTION_PROBABILITY), null));
   }

   @ManagedAttribute(description = "Lock Contention Probability",
                     displayName = "Lock Contention Probability")
   public double getLockContentionProbability() {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute((LOCK_CONTENTION_PROBABILITY), null));
   }

   @ManagedAttribute(description = "Avg lock holding time (in microseconds)",
                     displayName = "Avg lock holding time")
   public long getAvgLockHoldTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(LOCK_HOLD_TIME, null));
   }

   @ManagedAttribute(description = "Average lock local holding time (in microseconds)",
                     displayName = "Avg Lock Local Holding Time")
   public long getAvgLocalLockHoldTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(LOCK_HOLD_TIME_LOCAL, null));
   }

   @ManagedAttribute(description = "Average lock local holding time of committed tx (in microseconds)",
                     displayName = "Avg lock local holding time of committed txs")
   public long getAvgLocalSuccessfulLockHoldTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(SUX_LOCK_HOLD_TIME, null));
   }

   @ManagedAttribute(description = "Average lock local holding time of locally aborted tx (in microseconds)",
                     displayName = "Avg lock local holding time of locally aborted txs")
   public long getAvgLocalLocalAbortLockHoldTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(LOCAL_ABORT_LOCK_HOLD_TIME, null));
   }

   @ManagedAttribute(description = "Average lock local holding time of remotely aborted tx (in microseconds)",
                     displayName = "Avg lock local holding time of remotely aborted tx")
   public long getAvgLocalRemoteAbortLockHoldTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(REMOTE_ABORT_LOCK_HOLD_TIME, null));
   }

   @ManagedAttribute(description = "Average lock remote holding time (in microseconds)",
                     displayName = "Avg Lock Remote Holding Time")
   public long getAvgRemoteLockHoldTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(LOCK_HOLD_TIME_REMOTE, null));
   }

   @ManagedAttribute(description = "Avg prepare command size (in bytes)",
                     displayName = "Avg prepare command size (in bytes)")
   public long getAvgPrepareCommandSize() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(PREPARE_COMMAND_SIZE, null));
   }

   @ManagedAttribute(description = "Avg rollback command size (in bytes)",
                     displayName = "Avg rollback command size (in bytes)")
   public long getAvgRollbackCommandSize() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(ROLLBACK_COMMAND_SIZE, null));
   }

   @ManagedAttribute(description = "Avg size of a remote get reply (in bytes)",
                     displayName = "Avg size of a remote get reply (in bytes)")
   public long getAvgClusteredGetCommandReplySize() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(REMOTE_REMOTE_GET_REPLY_SIZE, null));
   }

   @ManagedAttribute(description = "Average commit command size (in bytes)",
                     displayName = "Avg Commit Command Size")
   public long getAvgCommitCommandSize() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(COMMIT_COMMAND_SIZE, null));
   }

   @ManagedAttribute(description = "Average clustered get command size (in bytes)",
                     displayName = "Avg Clustered Get Command Size")
   public long getAvgClusteredGetCommandSize() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(CLUSTERED_GET_COMMAND_SIZE, null));
   }

   @ManagedAttribute(description = "Average time waiting for the lock acquisition (in microseconds)",
                     displayName = "Avg Lock Waiting Time")
   public long getAvgLockWaitingTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(LOCK_WAITING_TIME, null));
   }

   @ManagedAttribute(description = "Average transaction arrival rate, originated locally and remotely (in transaction " +
         "per second)",
                     displayName = "Avg Transaction Arrival Rate")
   public double getAvgTxArrivalRate() {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(ARRIVAL_RATE, null));
   }

   @ManagedAttribute(description = "Percentage of Write transaction executed locally (committed and aborted)",
                     displayName = "Percentage of Write Transactions")
   public double getPercentageWriteTransactions() {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(TX_WRITE_PERCENTAGE, null));
   }

   @ManagedAttribute(description = "Percentage of Write transaction executed in all successfully executed " +
         "transactions (local transaction only)",
                     displayName = "Percentage of Successfully Write Transactions")
   public double getPercentageSuccessWriteTransactions() {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(SUCCESSFUL_WRITE_PERCENTAGE, null));
   }

   @ManagedAttribute(description = "The number of aborted transactions due to deadlock",
                     displayName = "No. of Aborted Transaction due to Deadlock")
   public long getNumAbortedTxDueDeadlock() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_LOCK_FAILED_DEADLOCK, null));
   }

   @ManagedAttribute(description = "Average successful read-only transaction duration (in microseconds)",
                     displayName = "Avg Read-Only Transaction Duration")
   public long getAvgReadOnlyTxDuration() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(RO_TX_SUCCESSFUL_EXECUTION_TIME, null));
   }

   @ManagedAttribute(description = "Average successful write transaction duration (in microseconds)",
                     displayName = "Avg Write Transaction Duration")
   public long getAvgWriteTxDuration() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(WR_TX_SUCCESSFUL_EXECUTION_TIME, null));
   }

   @ManagedAttribute(description = "Average aborted write transaction duration (in microseconds)",
                     displayName = "Avg Aborted Write Transaction Duration")
   public long getAvgAbortedWriteTxDuration() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(WR_TX_ABORTED_EXECUTION_TIME, null));
   }

   @ManagedAttribute(description = "Average number of locks per write local transaction",
                     displayName = "Avg no. of Lock per Local Transaction")
   public long getAvgNumOfLockLocalTx() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_LOCK_PER_LOCAL_TX, null));
   }

   @ManagedAttribute(description = "Average number of locks per write remote transaction",
                     displayName = "Avg no. of Lock per Remote Transaction")
   public long getAvgNumOfLockRemoteTx() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_LOCK_PER_REMOTE_TX, null));
   }

   @ManagedAttribute(description = "Average number of locks per successfully write local transaction",
                     displayName = "Avg no. of Lock per Successfully Local Transaction")
   public long getAvgNumOfLockSuccessLocalTx() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_LOCK_PER_SUCCESS_LOCAL_TX, null));
   }

   @ManagedAttribute(description = "Average time it takes to execute the rollback command remotely (in microseconds)",
                     displayName = "Avg remote CompletionCommand execution time")
   public long getAvgRemoteTxCompleteNotifyTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(TX_COMPLETE_NOTIFY_EXECUTION_TIME, null));
   }

   @ManagedAttribute(description = "Abort Rate",
                     displayName = "Abort Rate")
   public double getAbortRate() {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(ABORT_RATE, null));
   }

   @ManagedAttribute(description = "Throughput (in transactions per second)",
                     displayName = "Throughput")
   public double getThroughput() {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(THROUGHPUT, null));
   }

   @ManagedAttribute(description = "Number of tx dead in remote prepare",
                     displayName = "No. of tx dead in remote prepare")
   public long getRemotelyDeadXact() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_REMOTELY_ABORTED, null));
   }

   @ManagedAttribute(description = "Number of early aborts",
                     displayName = "No. of early aborts")
   public long getNumEarlyAborts() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_EARLY_ABORTS, null));
   }

   @ManagedAttribute(description = "Number of tx dead while preparing locally",
                     displayName = "No. of tx dead while preparing locally")
   public long getNumLocalPrepareAborts() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_LOCALPREPARE_ABORTS, null));
   }

   @ManagedAttribute(description = "Number of update tx gone up to prepare phase",
                     displayName = "No. of update tx gone up to prepare phase")
   public long getUpdateXactToPrepare() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_UPDATE_TX_GOT_TO_PREPARE, null));
   }

   @ManagedAttribute(description = "Average number of get operations per local read transaction",
                     displayName = "Avg no. of get operations per local read transaction")
   public long getAvgGetsPerROTransaction() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_SUCCESSFUL_GETS_RO_TX, null));
   }

   @ManagedAttribute(description = "Average number of get operations per local write transaction",
                     displayName = "Avg no. of get operations per local write transaction")
   public long getAvgGetsPerWrTransaction() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_SUCCESSFUL_GETS_WR_TX, null));
   }

   @ManagedAttribute(description = "Average number of remote get operations per local write transaction",
                     displayName = "Avg no. of remote get operations per local write transaction")
   public double getAvgRemoteGetsPerWrTransaction() {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(NUM_SUCCESSFUL_REMOTE_GETS_WR_TX, null));
   }

   @ManagedAttribute(description = "Average number of remote get operations per local read transaction",
                     displayName = "Avg no. of remote get operations per local read transaction")
   public double getAvgRemoteGetsPerROTransaction() {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(NUM_SUCCESSFUL_REMOTE_GETS_RO_TX, null));
   }

   @ManagedAttribute(description = "Average cost of a remote get",
                     displayName = "Remote get cost")
   public long getRemoteGetResponseTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(LOCAL_REMOTE_GET_R, null));
   }

   @ManagedAttribute(description = "Average number of put operations per local write transaction",
                     displayName = "Avg no. of put operations per local write transaction")
   public double getAvgPutsPerWrTransaction() {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(NUM_SUCCESSFUL_PUTS_WR_TX, null));
   }

   @ManagedAttribute(description = "Average number of remote put operations per local write transaction",
                     displayName = "Avg no. of remote put operations per local write transaction")
   public double getAvgRemotePutsPerWrTransaction() {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(NUM_SUCCESSFUL_REMOTE_PUTS_WR_TX, null));
   }

   @ManagedAttribute(description = "Average cost of a remote put",
                     displayName = "Remote put cost")
   public long getRemotePutExecutionTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(REMOTE_PUT_EXECUTION, null));
   }

   @ManagedAttribute(description = "Number of gets performed since last reset",
                     displayName = "No. of Gets")
   public long getNumberOfGets() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_GET, null));
   }

   @ManagedAttribute(description = "Number of remote gets performed since last reset",
                     displayName = "No. of Remote Gets")
   public long getNumberOfRemoteGets() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_LOCAL_REMOTE_GET, null));
   }

   @ManagedAttribute(description = "Number of puts performed since last reset",
                     displayName = "No. of Puts")
   public long getNumberOfPuts() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_PUT, null));
   }

   @ManagedAttribute(description = "Number of remote puts performed since last reset",
                     displayName = "No. of Remote Puts")
   public long getNumberOfRemotePuts() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_REMOTE_PUT, null));
   }

   @ManagedAttribute(description = "Number of committed transactions since last reset",
                     displayName = "No. Of Commits")
   public long getNumberOfCommits() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_COMMITS, null));
   }

   @ManagedAttribute(description = "Number of local committed transactions since last reset",
                     displayName = "No. Of Local Commits")
   public long getNumberOfLocalCommits() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_LOCAL_COMMITS, null));
   }

   @ManagedAttribute(description = "Write skew probability",
                     displayName = "Write Skew Probability")
   public double getWriteSkewProbability() {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(WRITE_SKEW_PROBABILITY, null));
   }

   @ManagedOperation(description = "Reset all the statistics collected",
                     displayName = "Reset All Statistics")
   public void resetStatistics() {
      TransactionsStatisticsRegistry.reset();
   }

   @ManagedAttribute(description = "Average Local processing Get time (in microseconds)",
                     displayName = "Avg Local Get time")
   public long getAvgLocalGetTime() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute(LOCAL_GET_EXECUTION, null)));
   }

   @ManagedAttribute(description = "Average TCB time (in microseconds)",
                     displayName = "Avg TCB time")
   public long getAvgTCBTime() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute(TBC, null)));
   }

   @ManagedAttribute(description = "Average NTCB time (in microseconds)",
                     displayName = "Avg NTCB time")
   public long getAvgNTCBTime() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute(NTBC, null)));
   }

   @ManagedAttribute(description = "Number of replicas for each key",
                     displayName = "Replication Degree")
   public long getReplicationDegree() {
      try {
         return distributionManager == null ? 1 : distributionManager.getConsistentHash().getNumOwners();
      } catch (Throwable throwable) {
         log.error("Error obtaining Replication Degree. returning 0", throwable);
         return 0;
      }
   }

   @ManagedAttribute(description = "Cost of terminating a tx (debug only)",
                     displayName = "Cost of terminating a tx (debug only)")
   public long getTerminationCost() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(TERMINATION_COST, null));
   }

   @ManagedAttribute(description = "Number of concurrent transactions executing on the current node",
                     displayName = "Local Active Transactions")
   public long getLocalActiveTransactions() {
      try {
         return transactionTable == null ? 0 : transactionTable.getLocalTxCount();
      } catch (Throwable throwable) {
         log.error("Error obtaining Local Active Transactions. returning 0", throwable);
         return 0;
      }
   }




    /*Local Update*/

   @ManagedAttribute(description = "Avg CPU demand to execute a local tx up to the prepare phase",
                     displayName = "Avg CPU to execute a local tx up to the prepare phase")
   public long getLocalUpdateTxLocalServiceTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(UPDATE_TX_LOCAL_S, null));
   }

   @ManagedAttribute(description = "Avg response time of the local part of a tx up to the prepare phase",
                     displayName = "Avg response time of a tx up to the prepare phase")
   public long getLocalUpdateTxLocalResponseTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(UPDATE_TX_LOCAL_R, null));
   }

   @ManagedAttribute(description = "Avg response time of a local PrepareCommand",
                     displayName = "Avg response time of a local PrepareCommand")
   public long getLocalUpdateTxPrepareResponseTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(UPDATE_TX_LOCAL_PREPARE_R, null));
   }

   @ManagedAttribute(description = "Avg CPU demand to execute a local PrepareCommand",
                     displayName = "Avg CPU to execute a local PrepareCommand")
   public long getLocalUpdateTxPrepareServiceTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(UPDATE_TX_LOCAL_PREPARE_S, null));
   }

   @ManagedAttribute(description = "Avg CPU demand to execute a local PrepareCommand",
                     displayName = "Avg CPU to execute a local PrepareCommand")
   public long getLocalUpdateTxCommitServiceTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(UPDATE_TX_LOCAL_COMMIT_S, null));
   }

   @ManagedAttribute(description = "Avg response time of a local CommitCommand",
                     displayName = "Avg response time of a local CommitCommand")
   public long getLocalUpdateTxCommitResponseTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(UPDATE_TX_LOCAL_COMMIT_R, null));
   }

   @ManagedAttribute(description = "Avg CPU demand to execute a local RollbackCommand without remote nodes",
                     displayName = "Avg CPU to execute a local RollbackCommand without remote nodes")
   public long getLocalUpdateTxLocalRollbackServiceTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(UPDATE_TX_LOCAL_LOCAL_ROLLBACK_S, null));
   }

   @ManagedAttribute(description = "Avg response time to execute a remote RollbackCommand",
                     displayName = "Avg response time to execute a remote RollbackCommand")
   public long getLocalUpdateTxLocalRollbackResponseTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(UPDATE_TX_LOCAL_LOCAL_ROLLBACK_R, null));
   }

   @ManagedAttribute(description = "Avg CPU demand to execute a local RollbackCommand with remote nodes",
                     displayName = "Avg CPU to execute a local RollbackCommand with remote nodes")
   public long getLocalUpdateTxRemoteRollbackServiceTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(UPDATE_TX_LOCAL_REMOTE_ROLLBACK_S, null));
   }

   @ManagedAttribute(description = "Avg response time to execute a local RollbackCommand with remote nodes",
                     displayName = "Avg response time of a local RollbackCommand with remote nodes")
   public long getLocalUpdateTxRemoteRollbackResponseTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(UPDATE_TX_LOCAL_REMOTE_ROLLBACK_R, null));
   }


   /*Remote*/

   @ManagedAttribute(description = "Avg response time of the prepare of a remote tx",
                     displayName = "Avg response time of the prepare of a remote tx")
   public long getRemoteUpdateTxPrepareResponseTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(UPDATE_TX_REMOTE_PREPARE_R, null));
   }

   @ManagedAttribute(description = "Avg CPU demand to execute the prepare of a remote tx",
                     displayName = "Avg CPU to execute the prepare of a remote tx")
   public long getRemoteUpdateTxPrepareServiceTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(UPDATE_TX_REMOTE_PREPARE_S, null));
   }

   @ManagedAttribute(description = "Avg CPU demand to execute the commitCommand of a remote tx",
                     displayName = "Avg CPU to execute the commitCommand of a remote tx")
   public long getRemoteUpdateTxCommitServiceTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(UPDATE_TX_REMOTE_COMMIT_S, null));
   }

   @ManagedAttribute(description = "Avg response time of the execution of the commitCommand of a remote tx",
                     displayName = "Avg response time of the CommitCommand execution for remote tx")
   public long getRemoteUpdateTxCommitResponseTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(UPDATE_TX_REMOTE_COMMIT_R, null));
   }

   @ManagedAttribute(description = "Avg CPU demand to perform the rollback of a remote tx",
                     displayName = "Avg CPU to perform the rollback of a remote tx")
   public long getRemoteUpdateTxRollbackServiceTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(UPDATE_TX_REMOTE_ROLLBACK_S, null));
   }

   @ManagedAttribute(description = "Avg response time of the rollback of a remote tx",
                     displayName = "Avg response time of the rollback of a remote tx")
   public long getRemoteUpdateTxRollbackResponseTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(UPDATE_TX_REMOTE_ROLLBACK_R, null));
   }

   /* Read Only*/

   @ManagedAttribute(description = "Avg CPU demand to perform the local execution of a read only tx",
                     displayName = "Avg CPU to perform the local execution of a read only tx")
   public long getLocalReadOnlyTxLocalServiceTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(READ_ONLY_TX_LOCAL_S, null));
   }

   @ManagedAttribute(description = "Avg response time of the local execution of a read only tx",
                     displayName = "Avg response time of local execution of a read only tx")
   public long getLocalReadOnlyTxLocalResponseTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(READ_ONLY_TX_LOCAL_R, null));
   }

   @ManagedAttribute(description = "Avg response time of the prepare of a read only tx",
                     displayName = "Avg response time of the prepare of a read only tx")
   public long getLocalReadOnlyTxPrepareResponseTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(READ_ONLY_TX_PREPARE_R, null));
   }

   @ManagedAttribute(description = "Avg CPU demand to perform the prepare of a read only tx",
                     displayName = "Avg CPU to perform the prepare of a read only tx")
   public long getLocalReadOnlyTxPrepareServiceTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(READ_ONLY_TX_PREPARE_S, null));
   }

   @ManagedAttribute(description = "Avg CPU demand to perform the commit of a read only tx",
                     displayName = "Avg CPU to perform the commit of a read only tx")
   public long getLocalReadOnlyTxCommitServiceTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(READ_ONLY_TX_COMMIT_S, null));
   }

   @ManagedAttribute(description = "Avg response time of the commit of a read only tx",
                     displayName = "Avg response time of the commit of a read only tx")
   public long getLocalReadOnlyTxCommitResponseTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(READ_ONLY_TX_COMMIT_R, null));
   }

   @ManagedAttribute(description = "Avg CPU demand to serve a remote get",
                     displayName = "Avg CPU to serve a remote get")
   public long getRemoteGetServiceTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(LOCAL_REMOTE_GET_S, null));
   }

   @ManagedAttribute(description = "Avg number of local items in a local prepareCommand",
                     displayName = "Avg no. or items in a local prepareCommand to write")
   public double getNumOwnedRdItemsInLocalPrepare() {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(NUM_OWNED_RD_ITEMS_IN_LOCAL_PREPARE, null));
   }

   @ManagedAttribute(description = "Avg number of local data items in a local prepareCommand to read-validate",
                     displayName = "Avg no. or items in a local prepareCommand to read")
   public double getNumOwnedWrItemsInLocalPrepare() {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(NUM_OWNED_WR_ITEMS_IN_LOCAL_PREPARE, null));
   }

   @ManagedAttribute(description = "Avg number of local data items in a remote prepareCommand to read-validate",
                     displayName = "Avg no. or items in a remote prepareCommand to read")
   public double getNumOwnedRdItemsInRemotePrepare() {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(NUM_OWNED_RD_ITEMS_IN_REMOTE_PREPARE, null));
   }

   @ManagedAttribute(description = "Avg number of local data items in a remote prepareCommand to write",
                     displayName = "Avg no. or items in a remote prepareCommand to write")
   public double getNumOwnedWrItemsInRemotePrepare() {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(NUM_OWNED_WR_ITEMS_IN_REMOTE_PREPARE, null));
   }

   @ManagedAttribute(description = "Avg waiting time before serving a ClusteredGetCommand",
                     displayName = "Avg waiting time before serving a ClusteredGetCommand")
   public long getWaitedTimeInRemoteCommitQueue() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(WAIT_TIME_IN_REMOTE_COMMIT_QUEUE, null));
   }

   @ManagedAttribute(displayName = "Avg time spent in the commit queue by a tx",
                     description = "Avg time spent in the commit queue by a tx")
   public long getWaitedTimeInLocalCommitQueue() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(WAIT_TIME_IN_COMMIT_QUEUE, null));
   }

   @ManagedAttribute(displayName = "No. of tx killed on the node due to unsuccessful validation",
                     description = "The number of tx killed on the node due to unsuccessful validation")
   public long getNumKilledTxDueToValidation() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_KILLED_TX_DUE_TO_VALIDATION, null));
   }

   @ManagedAttribute(description = "The number of aborted tx due to unsuccessful validation",
                     displayName = "No. of aborted tx due to unsuccessful validation")
   public long getNumAbortedTxDueToValidation() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_ABORTED_TX_DUE_TO_VALIDATION, null));
   }

   @ManagedAttribute(description = "The number of aborted tx due to timeout in readlock acquisition",
                     displayName = "No. of aborted tx due to timeout in readlock acquisition")
   public long getNumAbortedTxDueToReadLock() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_READLOCK_FAILED_TIMEOUT, null));
   }

   @ManagedAttribute(displayName = "No. of aborted tx due to timeout in lock acquisition",
                     description = "The number of aborted tx due to timeout in lock acquisition")
   public long getNumAbortedTxDueToWriteLock() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_LOCK_FAILED_TIMEOUT, null));
   }

   @ManagedAttribute(displayName = "No. of reads before first write",
                     description = "Number of reads before first writed")
   public double getNumReadsBeforeWrite() {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(FIRST_WRITE_INDEX, null));
   }

   @ManagedAttribute(displayName = "Avg waiting time for a GMUClusteredGetCommand",
                     description = "Avg waiting time for a GMUClusteredGetCommand")
   public long getGMUClusteredGetCommandWaitingTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(REMOTE_REMOTE_GET_WAITING_TIME, null));
   }

   @ManagedAttribute(displayName = "Avg response time for a GMUClusteredGetCommand (no queue)",
                     description = "Avg response time for a GMUClusteredGetCommand (no queue)")
   public long getGMUClusteredGetCommandResponseTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(REMOTE_REMOTE_GET_R, null));
   }

   @ManagedAttribute(displayName = "Avg cpu time for a GMUClusteredGetCommand",
                     description = "Avg cpu time for a GMUClusteredGetCommand")
   public long getGMUClusteredGetCommandServiceTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(REMOTE_REMOTE_GET_S, null));
   }

   @ManagedAttribute(displayName = "Avg CPU of a local update tx",
                     description = "Avg CPU demand of a local update tx")
   public long getLocalUpdateTxTotalCpuTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(UPDATE_TX_TOTAL_S, null));
   }

   @ManagedAttribute(displayName = "Avg response time of a local update tx",
                     description = "Avg response time of a local update tx")
   public long getLocalUpdateTxTotalResponseTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(UPDATE_TX_TOTAL_R, null));
   }

   @ManagedAttribute(displayName = "Avg CPU of a read only tx",
                     description = "Avg CPU demand of a read only tx")
   public long getReadOnlyTxTotalResponseTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(READ_ONLY_TX_TOTAL_R, null));
   }

   @ManagedAttribute(displayName = "Avg response time of a read only tx",
                     description = "Avg response time of a read only tx")
   public long getReadOnlyTxTotalCpuTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(READ_ONLY_TX_TOTAL_S, null));
   }

   @ManagedAttribute(description = "Avg Response Time",
                     displayName = "Avg Response Time")
   public long getAvgResponseTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(RESPONSE_TIME, null));
   }

   @ManagedOperation(description = "Average number of puts performed by a successful local transaction per class",
                     displayName = "No. of puts per successful local transaction per class")
   public double getAvgNumPutsBySuccessfulLocalTxForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(PUTS_PER_LOCAL_TX, txClass));
   }

   @ManagedOperation(description = "Average Prepare Round-Trip Time duration (in microseconds) per class",
                     displayName = "Avg Prepare RTT per class")
   public long getAvgPrepareRttForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute(RTT_PREPARE, txClass)));
   }

   @ManagedOperation(description = "Average Commit Round-Trip Time duration (in microseconds) per class",
                     displayName = "Avg Commit RTT per class")
   public long getAvgCommitRttForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute(RTT_COMMIT, txClass)));
   }

   @ManagedOperation(description = "Average Remote Get Round-Trip Time duration (in microseconds) per class",
                     displayName = "Avg Remote Get RTT per class")
   public long getAvgRemoteGetRttForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((RTT_GET), txClass)));
   }

   @ManagedOperation(description = "Average Rollback Round-Trip Time duration (in microseconds) per class",
                     displayName = "Avg Rollback RTT per class")
   public long getAvgRollbackRttForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((RTT_ROLLBACK), txClass)));
   }

   @ManagedOperation(description = "Average asynchronous Prepare duration (in microseconds) per class",
                     displayName = "Avg Prepare Async per class")
   public long getAvgPrepareAsyncForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((ASYNC_PREPARE), txClass)));
   }

   @ManagedOperation(description = "Average asynchronous Commit duration (in microseconds) per class",
                     displayName = "Avg Commit Async per class")
   public long getAvgCommitAsyncForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute(ASYNC_COMMIT, txClass)));
   }

   @ManagedOperation(description = "Average asynchronous Complete Notification duration (in microseconds) per class",
                     displayName = "Avg Complete Notification Async per class")
   public long getAvgCompleteNotificationAsyncForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute(ASYNC_COMPLETE_NOTIFY, txClass)));
   }

   @ManagedOperation(description = "Average asynchronous Rollback duration (in microseconds) per class",
                     displayName = "Avg Rollback Async per class")
   public long getAvgRollbackAsyncForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((ASYNC_ROLLBACK), txClass)));
   }

   @ManagedOperation(description = "Average number of nodes in Commit destination set per class",
                     displayName = "Avg no. of Nodes in Commit Destination Set per class")
   public long getAvgNumNodesCommitForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute(NUM_NODES_COMMIT, txClass)));
   }

   @ManagedOperation(description = "Average number of nodes in Complete Notification destination set per class",
                     displayName = "Avg no. of Nodes in Complete Notification Destination Set per class")
   public long getAvgNumNodesCompleteNotificationForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute(NUM_NODES_COMPLETE_NOTIFY, txClass)));
   }

   @ManagedOperation(description = "Average number of nodes in Remote Get destination set per class",
                     displayName = "Avg no. of Nodes in Remote Get Destination Set per class")
   public long getAvgNumNodesRemoteGetForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((NUM_NODES_GET), txClass)));
   }

   //JMX with Transactional class xxxxxxxxx

   @ManagedOperation(description = "Average number of nodes in Prepare destination set per class",
                     displayName = "Avg no. of Nodes in Prepare Destination Set per class")
   public long getAvgNumNodesPrepareForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute(NUM_NODES_PREPARE, txClass)));
   }

   @ManagedOperation(description = "Average number of nodes in Rollback destination set per class",
                     displayName = "Avg no. of Nodes in Rollback Destination Set per class")
   public long getAvgNumNodesRollbackForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((NUM_NODES_ROLLBACK), txClass)));
   }

   @ManagedOperation(description = "Application Contention Factor per class",
                     displayName = "Application Contention Factor per class")
   public double getApplicationContentionFactorForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(APPLICATION_CONTENTION_FACTOR, txClass));
   }

   @Deprecated
   @ManagedOperation(description = "Local Contention Probability per class",
                     displayName = "Local Conflict Probability per class")
   public double getLocalContentionProbabilityForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute((LOCAL_CONTENTION_PROBABILITY), txClass));
   }

   @Deprecated
   @ManagedOperation(description = "Remote Contention Probability per class",
                     displayName = "Remote Conflict Probability per class")
   public double getRemoteContentionProbabilityForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute((REMOTE_CONTENTION_PROBABILITY), txClass));
   }

   @ManagedOperation(description = "Lock Contention Probability per class",
                     displayName = "Lock Contention Probability per class")
   public double getLockContentionProbabilityForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute((LOCK_CONTENTION_PROBABILITY), txClass));
   }

   @ManagedOperation(description = "Average lock holding time (in microseconds) per class",
                     displayName = "Avg Lock Holding Time per class")
   public long getAvgLockHoldTimeForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(LOCK_HOLD_TIME, txClass));
   }

   @ManagedOperation(description = "Average lock local holding time (in microseconds) per class",
                     displayName = "Avg Lock Local Holding Time per class")
   public long getAvgLocalLockHoldTimeForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(LOCK_HOLD_TIME_LOCAL, txClass));
   }

   @ManagedOperation(description = "Average lock remote holding time (in microseconds) per class",
                     displayName = "Avg Lock Remote Holding Time per class")
   public long getAvgRemoteLockHoldTimeForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(LOCK_HOLD_TIME_REMOTE, txClass));
   }

   @ManagedOperation(description = "Average prepare command size (in bytes) per class",
                     displayName = "Avg Prepare Command Size per class")
   public long getAvgPrepareCommandSizeForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(PREPARE_COMMAND_SIZE, txClass));
   }

   @ManagedOperation(description = "Average commit command size (in bytes) per class",
                     displayName = "Avg Commit Command Size per class")
   public long getAvgCommitCommandSizeForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(COMMIT_COMMAND_SIZE, txClass));
   }

   @ManagedOperation(description = "Average clustered get command size (in bytes) per class",
                     displayName = "Avg Clustered Get Command Size per class")
   public long getAvgClusteredGetCommandSizeForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(CLUSTERED_GET_COMMAND_SIZE, txClass));
   }

   @ManagedOperation(description = "Average time waiting for the lock acquisition (in microseconds) per class",
                     displayName = "Avg Lock Waiting Time per class")
   public long getAvgLockWaitingTimeForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(LOCK_WAITING_TIME, txClass));
   }

   @ManagedOperation(description = "Average transaction arrival rate, originated locally and remotely (in transaction " +
         "per second) per class",
                     displayName = "Avg Transaction Arrival Rate per class")
   public double getAvgTxArrivalRateForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(ARRIVAL_RATE, txClass));
   }

   @ManagedOperation(description = "Percentage of Write tx executed locally (committed and aborted) per class",
                     displayName = "Percentage of Write Transactions per class")
   public double getPercentageWriteTransactionsForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(TX_WRITE_PERCENTAGE, txClass));
   }

   @ManagedOperation(description = "Percentage of Write tx executed in all successfully executed " +
         "xacts (local tx only) per class",
                     displayName = "Percentage of Successfully Write Transactions per class")
   public double getPercentageSuccessWriteTransactionsForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(SUCCESSFUL_WRITE_PERCENTAGE, txClass));
   }

   @ManagedOperation(description = "The number of aborted tx due to deadlock per class",
                     displayName = "No. of Aborted Transaction due to Deadlock per class")
   public long getNumAbortedTxDueDeadlockForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_LOCK_FAILED_DEADLOCK, txClass));
   }

   @ManagedOperation(description = "Average successful read-only tx duration (in microseconds) per class",
                     displayName = "Avg Read-Only Transaction Duration per class")
   public long getAvgReadOnlyTxDurationForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(RO_TX_SUCCESSFUL_EXECUTION_TIME, txClass));
   }

   @ManagedOperation(description = "Average successful write tx duration (in microseconds) per class",
                     displayName = "Avg Write Transaction Duration per class")
   public long getAvgWriteTxDurationForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(WR_TX_SUCCESSFUL_EXECUTION_TIME, txClass));
   }

   @ManagedOperation(description = "Average number of locks per write local tx per class",
                     displayName = "Avg no. of Lock per Local Transaction per class")
   public long getAvgNumOfLockLocalTxForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_LOCK_PER_LOCAL_TX, txClass));
   }

   @ManagedOperation(description = "Average number of locks per write remote tx per class",
                     displayName = "Avg no. of Lock per Remote Transaction per class")
   public long getAvgNumOfLockRemoteTxForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_LOCK_PER_REMOTE_TX, txClass));
   }

   @ManagedOperation(description = "Average number of locks per successfully write local transaction per class",
                     displayName = "Avg no. of Lock per Successfully Local Transaction per class")
   public long getAvgNumOfLockSuccessLocalTxForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_LOCK_PER_SUCCESS_LOCAL_TX, txClass));
   }

   @ManagedOperation(description = "Average time it takes to execute the rollback command remotely (in microseconds) " +
         "per class",
                     displayName = "Avg remote CompletionCommand execution time per tx class")
   public long getAvgRemoteTxCompleteNotifyTimeForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(TX_COMPLETE_NOTIFY_EXECUTION_TIME, txClass));
   }

   @ManagedOperation(description = "Abort Rate per class",
                     displayName = "Abort Rate per class")
   public double getAbortRateForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(ABORT_RATE, txClass));
   }

   @ManagedOperation(description = "Throughput (in tx per second) per class",
                     displayName = "Throughput per class")
   public double getThroughputForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(THROUGHPUT, txClass));
   }

   @ManagedOperation(description = "Average number of get operations per local read tx per class",
                     displayName = "Avg no. of get operations per local read tx per class")
   public long getAvgGetsPerROTransactionForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_SUCCESSFUL_GETS_RO_TX, txClass));
   }

   @ManagedOperation(description = "Average number of get operations per local write tx per class",
                     displayName = "Avg no. of get operations per local write tx per class")
   public long getAvgGetsPerWrTransactionForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_SUCCESSFUL_GETS_WR_TX, txClass));
   }

   @ManagedOperation(description = "Average number of remote get operations per local write tx per class",
                     displayName = "Avg no. of remote get operations per local write tx per class")
   public double getAvgRemoteGetsPerWrTransactionForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(NUM_SUCCESSFUL_REMOTE_GETS_WR_TX, txClass));
   }

   @ManagedOperation(description = "Average number of remote get operations per local read tx per class",
                     displayName = "Avg no. of remote get operations per local read tx per class")
   public double getAvgRemoteGetsPerROTransactionForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(NUM_SUCCESSFUL_REMOTE_GETS_RO_TX, txClass));
   }

   @ManagedOperation(description = "Average number of put operations per local write tx per class",
                     displayName = "Avg no. of put operations per local write tx per class")
   public double getAvgPutsPerWrTransactionForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(NUM_SUCCESSFUL_PUTS_WR_TX, txClass));
   }

   @ManagedOperation(description = "Average number of remote put operations per local write tx per class",
                     displayName = "Avg nr. of remote put operations per local write tx per tx class")
   public double getAvgRemotePutsPerWrTransactionForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(NUM_SUCCESSFUL_REMOTE_PUTS_WR_TX, txClass));
   }

   @ManagedOperation(description = "Average cost of a remote put per class",
                     displayName = "Remote put cost per class")
   public long getRemotePutExecutionTimeForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(REMOTE_PUT_EXECUTION, txClass));
   }

   @ManagedOperation(description = "Number of gets performed since last reset per class",
                     displayName = "No. of Gets per class")
   public long getNumberOfGetsForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_GET, txClass));
   }

   @ManagedOperation(description = "Number of remote gets performed since last reset per class",
                     displayName = "No. of Remote Gets per class")
   public long getNumberOfRemoteGetsForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_LOCAL_REMOTE_GET, txClass));
   }

   @ManagedOperation(description = "Number of puts performed since last reset per class",
                     displayName = "No. of Puts per class")
   public long getNumberOfPutsForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_PUT, txClass));
   }

   @ManagedOperation(description = "Number of remote puts performed since last reset per class",
                     displayName = "No. of Remote Puts per class")
   public long getNumberOfRemotePutsForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_REMOTE_PUT, txClass));
   }

   @ManagedOperation(description = "Number of committed tx since last reset per class",
                     displayName = "No. Of Commits per class")
   public long getNumberOfCommitsForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_COMMITS, txClass));
   }

   @ManagedOperation(description = "Number of local committed tx since last reset per class",
                     displayName = "No. Of Local Commits per class")
   public long getNumberOfLocalCommitsForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_LOCAL_COMMITS, txClass));
   }

   @ManagedOperation(description = "Write skew probability per class",
                     displayName = "Write Skew Probability per class")
   public double getWriteSkewProbabilityForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(WRITE_SKEW_PROBABILITY, txClass));
   }

   @ManagedOperation(description = "K-th percentile of local read-only tx execution time per class",
                     displayName = "K-th Percentile Local Read-Only Transactions per class")
   public double getPercentileLocalReadOnlyTransactionForTxClass(@Parameter(name = "Percentile") int percentile,
                                                                 @Parameter(name = "Transaction Class") String txClass) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getPercentile(RO_LOCAL_PERCENTILE, percentile, txClass));
   }

   @ManagedOperation(description = "K-th percentile of remote read-only tx execution time per tx class",
                     displayName = "K-th Percentile Remote Read-Only Transactions per tx class")
   public double getPercentileRemoteReadOnlyTransactionForTxClass(@Parameter(name = "Percentile") int percentile,
                                                                  @Parameter(name = "Transaction Class") String txClass) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getPercentile(RO_REMOTE_PERCENTILE, percentile, txClass));
   }

   @ManagedOperation(description = "K-th percentile of local write tx execution time per class",
                     displayName = "K-th Percentile Local Write Transactions per class")
   public double getPercentileLocalRWriteTransactionForTxClass(@Parameter(name = "Percentile") int percentile,
                                                               @Parameter(name = "Transaction Class") String txClass) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getPercentile(WR_LOCAL_PERCENTILE, percentile, txClass));
   }

   @ManagedOperation(description = "K-th percentile of remote write tx execution time per class",
                     displayName = "K-th Percentile Remote Write Transactions per class")
   public double getPercentileRemoteWriteTransactionForTxClass(@Parameter(name = "Percentile") int percentile,
                                                               @Parameter(name = "Transaction Class") String txClass) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getPercentile(WR_REMOTE_PERCENTILE, percentile, txClass));
   }

   @ManagedOperation(description = "Average Local processing Get time (in microseconds) per class",
                     displayName = "Avg Local Get time per class")
   public long getAvgLocalGetTimeForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute(LOCAL_GET_EXECUTION, txClass)));
   }

   @ManagedOperation(description = "K-th percentile of local read-only transactions execution time",
                     displayName = "K-th Percentile Local Read-Only Transactions")
   public double getPercentileLocalReadOnlyTransaction(@Parameter(name = "Percentile") int percentile) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getPercentile(RO_LOCAL_PERCENTILE, percentile, null));
   }

   @ManagedOperation(description = "K-th percentile of remote read-only transactions execution time",
                     displayName = "K-th Percentile Remote Read-Only Transactions")
   public double getPercentileRemoteReadOnlyTransaction(@Parameter(name = "Percentile") int percentile) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getPercentile(RO_REMOTE_PERCENTILE, percentile, null));
   }

   @ManagedOperation(description = "K-th percentile of local write transactions execution time",
                     displayName = "K-th Percentile Local Write Transactions")
   public double getPercentileLocalRWriteTransaction(@Parameter(name = "Percentile") int percentile) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getPercentile(WR_LOCAL_PERCENTILE, percentile, null));
   }

   @ManagedOperation(description = "K-th percentile of remote write transactions execution time",
                     displayName = "K-th Percentile Remote Write Transactions")
   public double getPercentileRemoteWriteTransaction(@Parameter(name = "Percentile") int percentile) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getPercentile(WR_REMOTE_PERCENTILE, percentile, null));
   }

   @ManagedOperation(description = "Average TCB time (in microseconds) per class",
                     displayName = "Avg TCB time per class")
   public long getAvgTCBTimeForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute(TBC, txClass)));
   }

   @ManagedOperation(description = "Average NTCB time (in microseconds) per class",
                     displayName = "Avg NTCB time per class")
   public long getAvgNTCBTimeForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute(NTBC, txClass)));
   }

   @ManagedAttribute(description = "Number of local aborted txs",
                     displayName = "No. of local aborted txs")
   public long getNumAbortedXacts() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((NUM_ABORTED_WR_TX), null)));
   }

   @ManagedAttribute(description = "Number of remote gets that have waited",
                     displayName = "No. of remote gets that have waited")
   public long getNumWaitedRemoteGets() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((NUM_WAITS_REMOTE_REMOTE_GETS), null)));
   }

   @ManagedAttribute(description = "Number of local commits that have waited",
                     displayName = "No. of local commits that have waited")
   public long getNumWaitedLocalCommits() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((NUM_WAITS_IN_COMMIT_QUEUE), null)));
   }

   @ManagedAttribute(description = "Number of remote commits that have waited",
                     displayName = "No. of remote commits that have waited")
   public long getNumWaitedRemoteCommits() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((NUM_WAITS_IN_REMOTE_COMMIT_QUEUE), null)));
   }

   @ManagedAttribute(description = "Local Local Contentions",
                     displayName = "Local Local Contentions")
   public long getNumLocalLocalLockContentions() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((LOCK_CONTENTION_TO_LOCAL), null)));
   }

   @ManagedAttribute(description = "Local Remote Contentions",
                     displayName = "Local Remote Contentions")
   public long getNumLocalRemoteLockContentions() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((LOCK_CONTENTION_TO_REMOTE), null)));
   }

   @ManagedAttribute(description = "Remote Local Contentions",
                     displayName = "Remote Local Contentions")
   public long getNumRemoteLocalLockContentions() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((REMOTE_LOCK_CONTENTION_TO_LOCAL), null)));
   }

   @ManagedAttribute(description = "Remote Remote Contentions",
                     displayName = "Remote Remote Contentions")
   public long getNumRemoteRemoteLockContentions() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((REMOTE_LOCK_CONTENTION_TO_REMOTE), null)));
   }

   @ManagedAttribute(description = "Average local Tx Waiting time due to pending Tx",
                     displayName = "Avg local Tx Waiting time due to pending Tx")
   public final long getAvgLocalTxWaitingTimeInGMUQueueDuePendingTx() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(GMU_WAITING_IN_QUEUE_DUE_PENDING_LOCAL, null));
   }

   @ManagedAttribute(description = "Average remote Tx Waiting time due to pending Tx",
                     displayName = "Avg remote Tx Waiting time due to pending Tx")
   public final long getAvgRemoteTxWaitingTimeInGMUQueueDuePendingTx() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(GMU_WAITING_IN_QUEUE_DUE_PENDING_REMOTE, null));
   }

   @ManagedAttribute(description = "Average local Tx Waiting time due to slow commits",
                     displayName = "Avg local Tx Waiting time due to slow commits")
   public final long getAvgLocalTxWaitingTimeInGMUQueueDueToSlowCommits() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(GMU_WAITING_IN_QUEUE_DUE_SLOW_COMMITS_LOCAL, null));
   }

   @ManagedAttribute(description = "Average remote Tx Waiting time due to slow commits",
                     displayName = "Avg remote Tx Waiting time due to slow commits")
   public final long getAvgRemoteTxWaitingTimeInGMUQueueDueToSlowCommits() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(GMU_WAITING_IN_QUEUE_DUE_SLOW_COMMITS_REMOTE, null));
   }

   @ManagedAttribute(description = "Average local Tx Waiting time due to commit conflicts",
                     displayName = "Avg local Tx Waiting time due to commit conflicts")
   public final long getAvgLocalTxWaitingTimeInGMUQueueDueToCommitsConflicts() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(GMU_WAITING_IN_QUEUE_DUE_CONFLICT_VERSION_LOCAL, null));
   }

   @ManagedAttribute(description = "Average remote Tx Waiting time due to commit conflicts",
                     displayName = "Avg remote Tx Waiting time due to commit conflicts")
   public final long getAvgRemoteTxWaitingTimeInGMUQueueDueToCommitsConflicts() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(GMU_WAITING_IN_QUEUE_DUE_CONFLICT_VERSION_REMOTE, null));
   }

   @ManagedAttribute(description = "Number of local Tx that waited due to pending Tx",
                     displayName = "No. of local Tx that waited due to pending Tx")
   public final long getNumLocalTxWaitingTimeInGMUQueueDuePendingTx() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_GMU_WAITING_IN_QUEUE_DUE_PENDING_LOCAL, null));
   }

   @ManagedAttribute(description = "Number of remote Tx that waited due to pending Tx",
                     displayName = "No. of remote Tx that waited due to pending Tx")
   public final long getNumRemoteTxWaitingTimeInGMUQueueDuePendingTx() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_GMU_WAITING_IN_QUEUE_DUE_PENDING_REMOTE, null));
   }

   @ManagedAttribute(description = "Number of local Tx that waited due to slow commits",
                     displayName = "No. of local Tx that waited due to slow commits")
   public final long getNumLocalTxWaitingTimeInGMUQueueDueToSlowCommits() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_GMU_WAITING_IN_QUEUE_DUE_SLOW_COMMITS_LOCAL, null));
   }

   @ManagedAttribute(description = "Number of remote Tx that waited due to slow commits",
                     displayName = "No. of remote Tx that waited due to slow commits")
   public final long getNumRemoteTxWaitingTimeInGMUQueueDueToSlowCommits() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_GMU_WAITING_IN_QUEUE_DUE_SLOW_COMMITS_REMOTE, null));
   }

   @ManagedAttribute(description = "Number of local Tx that waited due to commit conflicts",
                     displayName = "No. of local Tx that waited due to commit conflicts")
   public final long getNumLocalTxWaitingTimeInGMUQueueDueToCommitsConflicts() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_GMU_WAITING_IN_QUEUE_DUE_CONFLICT_VERSION_LOCAL, null));
   }

   @ManagedAttribute(description = "Number of remote Tx that waited due to commit conflicts",
                     displayName = "No. of remote Tx that waited due to commit conflicts")
   public final long getNumRemoteTxWaitingTimeInGMUQueueDueToCommitsConflicts() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_GMU_WAITING_IN_QUEUE_DUE_CONFLICT_VERSION_REMOTE, null));
   }

   @ManagedAttribute(description = "Returns true if the statistics are enabled, false otherwise",
                     displayName = "Enabled")
   public final boolean isEnabled() {
      return TransactionsStatisticsRegistry.isActive();
   }

   @ManagedOperation(description = "Enabled/Disables the statistic collection.",
                     displayName = "Enable/Disable Statistic")
   public final void setEnabled(@Parameter boolean enabled) {
      TransactionsStatisticsRegistry.setActive(enabled);
   }

   @ManagedAttribute(description = "Returns true if the waiting time in GMU queue statistics are collected",
                     displayName = "Waiting time in GMU enabled")
   public final boolean isGMUWaitingTimeEnabled() {
      return TransactionsStatisticsRegistry.isGmuWaitingActive();
   }

   @ManagedOperation(description = "Enables/Disables the waiting in time GMU queue collection",
                     displayName = "Enable/Disable GMU Waiting statistics")
   public final void setGMUWaitingTimeEnabled(@Parameter boolean enabled) {
      TransactionsStatisticsRegistry.setGmuWaitingActive(enabled);
   }

   @ManagedAttribute(description = "Avg Conditional Waiting time experience by TO-GMU prepare command",
                     displayName = "Avg Conditional Waiting time experience by TO-GMU prepare command")
   public final long getTOPrepareWaitTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(TO_GMU_PREPARE_COMMAND_AVG_WAIT_TIME, null));
   }

   @ManagedAttribute(description = "Average TO-GMU conditional max replay time at cohorts",
                     displayName = "Avg TO-GMU conditional max replay time at cohorts")
   public final long getTOMaxSuccessfulValidationWaitingTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(TO_GMU_PREPARE_COMMAND_MAX_WAIT_TIME, null));
   }

   @ManagedAttribute(description = "Average TO-GMU validation conditional wait time on a single node",
                     displayName = "Avg TO-GMU validation conditional wait time on a single node")
   public final long getTOAvgValidationConditionalWaitTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(TO_GMU_PREPARE_COMMAND_REMOTE_WAIT, null));
   }

   @ManagedAttribute(description = "Average TO-GMU validations that waited on a node",
                     displayName = "Avg TO-GMU validations that waited on a node")
   public final long getTONumValidationWaited() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_TO_GMU_PREPARE_COMMAND_REMOTE_WAITED, null));
   }

   @ManagedAttribute(description = "Probability that a TO-GMU prepare experiences waiting time",
                     displayName = "Probability that a TO-GMU prepare experiences waiting time")
   public final long getTOGMUPrepareWaitProbability() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_TO_GMU_PREPARE_COMMAND_AT_LEAST_ONE_WAIT, null));
   }

   @ManagedAttribute(description = "Average TO-GMU prepare Rtt minus avg replay time at cohorts for successful tx",
                     displayName = "Avg TO-GMU Prepare RTT (- avg replay) time")
   public final long getTOGMUPrepareRttMinusAvgValidationTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(TO_GMU_PREPARE_COMMAND_RTT_MINUS_AVG, null));
   }

   @ManagedAttribute(description = "Average TO-GMU prepare Rtt minus max replay time at cohorts for successful tx",
                     displayName = "Avg TO-GMU Prepare RTT (- max replay) time")
   public final long getTOGMUPrepareRttMinusMaxValidationTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(TO_GMU_PREPARE_COMMAND_RTT_MINUS_MAX, null));
   }

   @ManagedAttribute(description = "Average TO-GMU mean replay time at cohorts for successful tx",
                     displayName = "Avg TO-GMU mean replay time at cohorts for successful tx")
   public final long getTOGMUAvgSuccessfulValidationResponseTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(TO_GMU_PREPARE_COMMAND_RESPONSE_TIME, null));
   }

   @ManagedAttribute(description = "Average TO-GMU mean replay time at cohorts for successful tx",
                     displayName = "Avg TO-GMU mean replay time at cohorts for successful tx")
   public final long getTOGMUAvgSuccessfulValidationServiceTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(TO_GMU_PREPARE_COMMAND_SERVICE_TIME, null));
   }

   @ManagedAttribute(description = "Number of successful TO-GMU prepares that did not wait",
                     displayName = "No. of successful TO-GMU prepares that did not wait")
   public final long getNumTOGMUSuccessfulPrepareNoWait() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_TO_GMU_PREPARE_COMMAND_RTT_NO_WAITED, null));
   }

   @ManagedAttribute(description = "Avg rtt for TO-GMU prepares that did not wait",
                     displayName = "Avg rtt for TO-GMU prepares that did not wait")
   public final long getAvgTOGMUPrepareNoWaitRtt() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(TO_GMU_PREPARE_COMMAND_RTT_NO_WAIT, null));
   }

   @ManagedAttribute(description = "Avg rtt for GMU remote get that do not wait",
                     displayName = "Avg rtt for GMU remote get that do not wait")
   public final long getAvgGmuClusteredGetCommandRttNoWait() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(RTT_GET_NO_WAIT, null));
   }

    /*
     "handleCommand" methods could have been more compact (since they do very similar stuff)
     But I wanted smaller, clear sub-methods
    */

   @ManagedOperation(description = "Avg CPU demand to perform the commit of a read only tx per tx class",
                     displayName = "Avg CPU to perform the commit of a read only tx per tx class")
   public final long getLocalReadOnlyTxCommitServiceTimeForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(READ_ONLY_TX_COMMIT_S, txClass));
   }

   @ManagedOperation(description = "Avg waiting time for a GMUClusteredGetCommand per tx class",
                     displayName = "Avg waiting time for a GMUClusteredGetCommand per tx class")
   public final long getGMUClusteredGetCommandWaitingTimeForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(REMOTE_REMOTE_GET_WAITING_TIME, txClass));
   }

   @ManagedOperation(description = "Avg response time of the prepare of a remote tx per tx class",
                     displayName = "Avg response time of the prepare of a remote tx per tx class")
   public final long getRemoteUpdateTxPrepareResponseTimeForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(UPDATE_TX_REMOTE_PREPARE_R, txClass));
   }

   @ManagedOperation(description = "The number of tx killed on the node due to unsuccessful validation per tx class",
                     displayName = "No. of tx killed due to unsuccessful validation per tx class")
   public final long getNumKilledTxDueToValidationForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_KILLED_TX_DUE_TO_VALIDATION, txClass));
   }

   @ManagedOperation(description = "Avg response time of the prepare of a read only tx per tx class",
                     displayName = "Avg response time of the prepare of a read only tx per tx class")
   public final long getLocalReadOnlyTxPrepareResponseTimeForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(READ_ONLY_TX_PREPARE_R, txClass));
   }

   @ManagedOperation(description = "Number of local Tx that waited due to slow commits per tx class",
                     displayName = "No. of local Tx that waited due to slow commits per tx class")
   public final long getNumLocalTxWaitingTimeInGMUQueueDueToSlowCommitsForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_GMU_WAITING_IN_QUEUE_DUE_SLOW_COMMITS_LOCAL, txClass));
   }

   @ManagedOperation(description = "Average local Tx Waiting time due to commit conflicts per tx class",
                     displayName = "Avg local Tx Waiting time due to commit conflicts per tx class")
   public final long getAvgLocalTxWaitingTimeInGMUQueueDueToCommitsConflictsForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(GMU_WAITING_IN_QUEUE_DUE_CONFLICT_VERSION_LOCAL, txClass));
   }

   @ManagedOperation(description = "Avg CPU demand to execute the commitCommand of a remote tx per tx class",
                     displayName = "Avg CPU to execute the CommitCommand of a remote tx per tx class")
   public final long getRemoteUpdateTxCommitServiceTimeForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(UPDATE_TX_REMOTE_COMMIT_S, txClass));
   }

   @ManagedOperation(description = "Average TO-GMU prepare Rtt minus max replay time at cohorts for successful tx per tx class",
                     displayName = "Avg TO-GMU Prepare RTT (- max replay) time per tx class")
   public final long getTOGMUPrepareRttMinusMaxValidationTimeForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(TO_GMU_PREPARE_COMMAND_RTT_MINUS_MAX, txClass));
   }

   @ManagedOperation(description = "Remote Remote Contentions per tx class",
                     displayName = "Remote Remote Contentions per tx class")
   public final long getNumRemoteRemoteLockContentionsForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(REMOTE_LOCK_CONTENTION_TO_REMOTE, txClass));
   }

   @ManagedOperation(description = "Avg CPU demand of a local update tx per tx class",
                     displayName = "Avg CPU of a local update tx per tx class")
   public final long getLocalUpdateTxTotalCpuTimeForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(UPDATE_TX_TOTAL_S, txClass));
   }

   @ManagedOperation(description = "Number of local Tx that waited due to commit conflicts per tx class",
                     displayName = "No. of local Tx that waited due to commit conflicts per tx class")
   public final long getNumLocalTxWaitingTimeInGMUQueueDueToCommitsConflictsForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_GMU_WAITING_IN_QUEUE_DUE_CONFLICT_VERSION_LOCAL, txClass));
   }

   @ManagedOperation(description = "Avg CPU demand to perform the prepare of a read only tx per tx class",
                     displayName = "Avg CPU to perform the a read only tx Prepare per tx class")
   public final long getLocalReadOnlyTxPrepareServiceTimeForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(READ_ONLY_TX_PREPARE_S, txClass));
   }

   @ManagedOperation(description = "Avg time spent in the commit queue by a tx per tx class",
                     displayName = "Avg time spent in the commit queue by a tx per tx class")
   public final long getWaitedTimeInLocalCommitQueueForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(WAIT_TIME_IN_COMMIT_QUEUE, txClass));
   }

   @ManagedOperation(description = "Avg CPU demand to execute a local tx up to the prepare phase per tx class",
                     displayName = "Avg CPU to execute a local tx up to the prepare phase per tx class")
   public final long getLocalUpdateTxLocalServiceTimeForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(UPDATE_TX_LOCAL_S, txClass));
   }

   @ManagedOperation(description = "Avg CPU demand to execute the prepare of a remote tx per tx class",
                     displayName = "Avg CPU to execute the prepare of a remote tx per tx class")
   public final long getRemoteUpdateTxPrepareServiceTimeForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(UPDATE_TX_REMOTE_PREPARE_S, txClass));
   }

   @ManagedOperation(description = "Number of local commits that have waited per tx class",
                     displayName = "No. of local commits that have waited per tx class")
   public final long getNumWaitedLocalCommitsForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_WAITS_IN_COMMIT_QUEUE, txClass));
   }

   @ManagedOperation(description = "Avg CPU demand to execute a local PrepareCommand per tx class",
                     displayName = "Avg CPU to execute a local PrepareCommand per tx class")
   public final long getLocalUpdateTxCommitServiceTimeForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(UPDATE_TX_LOCAL_COMMIT_S, txClass));
   }

   @ManagedOperation(description = "Avg rtt for TO-GMU prepares that did not wait per tx class",
                     displayName = "Avg rtt for TO-GMU prepares that did not wait per tx class")
   public final long getAvgTOGMUPrepareNoWaitRttForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(TO_GMU_PREPARE_COMMAND_RTT_NO_WAIT, txClass));
   }

   @ManagedOperation(description = "Number of remote Tx that waited due to pending Tx per tx class",
                     displayName = "No. of remote Tx that waited due to pending Tx per tx class")
   public final long getNumRemoteTxWaitingTimeInGMUQueueDuePendingTxForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_GMU_WAITING_IN_QUEUE_DUE_PENDING_REMOTE, txClass));
   }

   @ManagedOperation(description = "Number of successful TO-GMU prepares that did not wait per tx class",
                     displayName = "No. of successful TO-GMU prepares that did not wait per tx class")
   public final long getNumTOGMUSuccessfulPrepareNoWaitForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_TO_GMU_PREPARE_COMMAND_RTT_NO_WAITED, txClass));
   }

   @ManagedOperation(description = "The number of aborted tx due to timeout in lock acquisition per tx class",
                     displayName = "No. of aborted tx due to timeout in lock acquisition per tx class")
   public final long getNumAbortedTxDueToWriteLockForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_LOCK_FAILED_TIMEOUT, txClass));
   }

   @ManagedOperation(description = "Avg CPU demand to execute a local RollbackCommand with remote nodes per tx class",
                     displayName = "Avg CPU to execute a local RollbackCommand with remote nodes per tx class")
   public final long getLocalUpdateTxRemoteRollbackServiceTimeForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(UPDATE_TX_LOCAL_REMOTE_ROLLBACK_S, txClass));
   }

   @ManagedOperation(description = "Average remote Tx Waiting time due to pending Tx per tx class",
                     displayName = "Avg remote Tx Waiting time due to pending Tx per tx class")
   public final long getAvgRemoteTxWaitingTimeInGMUQueueDuePendingTxForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(GMU_WAITING_IN_QUEUE_DUE_PENDING_REMOTE, txClass));
   }

   @ManagedOperation(description = "Average local Tx Waiting time due to pending Tx per tx class",
                     displayName = "Avg local Tx Waiting time due to pending Tx per tx class")
   public final long getAvgLocalTxWaitingTimeInGMUQueueDuePendingTxForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(GMU_WAITING_IN_QUEUE_DUE_PENDING_LOCAL, txClass));
   }

   @ManagedOperation(description = "Probability that a TO-GMU prepare experiences waiting time per tx class",
                     displayName = "Probability TO-GMU prepare experiences waiting time per tx class")
   public final long getTOGMUPrepareWaitProbabilityForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_TO_GMU_PREPARE_COMMAND_AT_LEAST_ONE_WAIT, txClass));
   }

   @ManagedOperation(description = "Average number of nodes of async commit messages sent per tx per tx class",
                     displayName = "Avg nr of nodes in async commit messages per tx class")
   public final long getAvgNumAsyncSentCommitMsgsForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(SENT_ASYNC_COMMIT, txClass));
   }

   @ManagedOperation(description = "Number of remote Tx that waited due to commit conflicts per tx class",
                     displayName = "No. of remote Tx that waited due to commit conflicts per tx class")
   public final long getNumRemoteTxWaitingTimeInGMUQueueDueToCommitsConflictsForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_GMU_WAITING_IN_QUEUE_DUE_CONFLICT_VERSION_REMOTE, txClass));
   }

   @ManagedOperation(description = "Avg TO-GMU mean replay time at cohorts for successful tx per tx class",
                     displayName = "Avg TO-GMU mean replay time at cohorts for successful tx per tx class")
   public final long getTOGMUAvgSuccessfulValidationResponseTimeForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(TO_GMU_PREPARE_COMMAND_RESPONSE_TIME, txClass));
   }

   @ManagedOperation(description = "Local Remote Contentions per tx class",
                     displayName = "Local Remote Contentions per tx class")
   public final long getNumLocalRemoteLockContentionsForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(LOCK_CONTENTION_TO_REMOTE, txClass));
   }

   @ManagedOperation(description = "Avg Response Time per tx class",
                     displayName = "Avg Response Time per tx class")
   public final long getAvgResponseTimeForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(RESPONSE_TIME, txClass));
   }

   @ManagedOperation(description = "Average TO-GMU validation conditional wait time on a single node per tx class",
                     displayName = "Avg TO-GMU validation conditional wait time on a single node per tx class")
   public final long getTOAvgValidationConditionalWaitTimeForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(TO_GMU_PREPARE_COMMAND_REMOTE_WAIT, txClass));
   }

   @ManagedOperation(description = "Avg number of local data items in a local prepareCommand to read-validate per tx class",
                     displayName = "Avg no. of items in a local prepareCommand to read per tx class")
   public final double getNumOwnedWrItemsInLocalPrepareForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(NUM_OWNED_WR_ITEMS_IN_LOCAL_PREPARE, txClass));
   }

   @ManagedOperation(description = "Avg rollback command size (in bytes) per tx class",
                     displayName = "Avg rollback command size (in bytes) per tx class")
   public final long getAvgRollbackCommandSizeForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(ROLLBACK_COMMAND_SIZE, txClass));
   }

   @ManagedOperation(description = "Number of reads before first writed per tx class",
                     displayName = "No. of reads before first write per tx class")
   public final double getNumReadsBeforeWriteForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(FIRST_WRITE_INDEX, txClass));
   }

   @ManagedOperation(description = "The number of aborted tx due to unsuccessful validation per tx class",
                     displayName = "No. of aborted tx due to unsuccessful validation per tx class")
   public final long getNumAbortedTxDueToValidationForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_ABORTED_TX_DUE_TO_VALIDATION, txClass));
   }

   @ManagedOperation(description = "Avg response time to execute a remote RollbackCommand per tx class",
                     displayName = "Avg response time to execute a remote RollbackCommand per tx class")
   public final long getLocalUpdateTxLocalRollbackResponseTimeForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(UPDATE_TX_LOCAL_LOCAL_ROLLBACK_R, txClass));
   }

   @ManagedOperation(description = "Avg CPU demand of a read only tx per tx class",
                     displayName = "Avg CPU of a read only tx per tx class")
   public final long getReadOnlyTxTotalResponseTimeForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(READ_ONLY_TX_TOTAL_R, txClass));
   }

   @ManagedOperation(description = "Avg CPU demand to perform the rollback of a remote tx per tx class",
                     displayName = "Avg CPU to perform the rollback of a remote tx per tx class")
   public final long getRemoteUpdateTxRollbackServiceTimeForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(UPDATE_TX_REMOTE_ROLLBACK_S, txClass));
   }

   @ManagedOperation(description = "Avg response time of the rollback of a remote tx per tx class",
                     displayName = "Avg response time of the rollback of a remote tx per tx class")
   public final long getRemoteUpdateTxRollbackResponseTimeForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(UPDATE_TX_REMOTE_ROLLBACK_R, txClass));
   }

   @ManagedOperation(description = "Avg number of local data items in a remote prepareCommand to write per tx class",
                     displayName = "Avg no. or items in a remote prepareCommand to write per tx class")
   public final double getNumOwnedWrItemsInRemotePrepareForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(NUM_OWNED_WR_ITEMS_IN_REMOTE_PREPARE, txClass));
   }

   @ManagedOperation(description = "Avg size of a remote get reply (in bytes) per tx class",
                     displayName = "Avg size of a remote get reply (in bytes) per tx class")
   public final long getAvgClusteredGetCommandReplySizeForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(REMOTE_REMOTE_GET_REPLY_SIZE, txClass));
   }

   @ManagedOperation(description = "Number of remote commits that have waited per tx class",
                     displayName = "No. of remote commits that have waited per tx class")
   public final long getNumWaitedRemoteCommitsForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_WAITS_IN_REMOTE_COMMIT_QUEUE, txClass));
   }

   @ManagedOperation(description = "Average lock local holding time of locally aborted tx (in microseconds) per tx class",
                     displayName = "Avg lock local holding time of locally aborted tx per tx class")
   public final long getAvgLocalLocalAbortLockHoldTimeForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(LOCAL_ABORT_LOCK_HOLD_TIME, txClass));
   }

   @ManagedOperation(description = "Average number of nodes of Sync commit messages sent per tx per tx class",
                     displayName = "Avg no. of nodes of Sync commit messages sent per tx per tx class")
   public final long getAvgNumSyncSentCommitMsgsForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(SENT_SYNC_COMMIT, txClass));
   }

   @ManagedOperation(description = "Number of remote Tx that waited due to slow commits per tx class",
                     displayName = "No. of remote Tx that waited due to slow commits per tx class")
   public final long getNumRemoteTxWaitingTimeInGMUQueueDueToSlowCommitsForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_GMU_WAITING_IN_QUEUE_DUE_SLOW_COMMITS_REMOTE, txClass));
   }

   @ManagedOperation(description = "Avg rtt for GMU remote get that do not wait per tx class",
                     displayName = "Avg rtt for GMU remote get that do not wait per tx class")
   public final long getAvgGmuClusteredGetCommandRttNoWaitForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(RTT_GET_NO_WAIT, txClass));
   }

   @ManagedOperation(description = "Avg response time of a read only tx per tx class",
                     displayName = "Avg response time of a read only tx per tx class")
   public final long getReadOnlyTxTotalCpuTimeForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(READ_ONLY_TX_TOTAL_S, txClass));
   }

   @ManagedOperation(description = "Local Local Contentions per tx class",
                     displayName = "Local Local Contentions per tx class")
   public final long getNumLocalLocalLockContentionsForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(LOCK_CONTENTION_TO_LOCAL, txClass));
   }

   @ManagedOperation(description = "Number of remote gets that have waited per tx class",
                     displayName = "No. of remote gets that have waited per tx class")
   public final long getNumWaitedRemoteGetsForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_WAITS_REMOTE_REMOTE_GETS, txClass));
   }

   @ManagedOperation(description = "Average TO-GMU conditional max replay time at cohorts per tx class",
                     displayName = "Avg TO-GMU conditional max replay time at cohorts per tx class")
   public final long getTOMaxSuccessfulValidationWaitingTimeForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(TO_GMU_PREPARE_COMMAND_MAX_WAIT_TIME, txClass));
   }

   @ManagedOperation(description = "Avg response time to execute a local RollbackCommand with remote nodes per tx class",
                     displayName = "Avg response time of a local RollbackCommand with remote nodes per tx class")
   public final long getLocalUpdateTxRemoteRollbackResponseTimeForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(UPDATE_TX_LOCAL_REMOTE_ROLLBACK_R, txClass));
   }

   @ManagedOperation(description = "Avg response time of a local update tx per tx class",
                     displayName = "Avg response time of a local update tx per tx class")
   public final long getLocalUpdateTxTotalResponseTimeForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(UPDATE_TX_TOTAL_R, txClass));
   }

   @ManagedOperation(description = "Average remote Tx Waiting time due to commit conflicts per tx class",
                     displayName = "Avg remote Tx Waiting time due to commit conflicts per tx class")
   public final long getAvgRemoteTxWaitingTimeInGMUQueueDueToCommitsConflictsForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(GMU_WAITING_IN_QUEUE_DUE_CONFLICT_VERSION_REMOTE, txClass));
   }

   @ManagedOperation(description = "Average remote Tx Waiting time due to slow commits per tx class",
                     displayName = "Avg remote Tx Waiting time due to slow commits per tx class")
   public final long getAvgRemoteTxWaitingTimeInGMUQueueDueToSlowCommitsForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(GMU_WAITING_IN_QUEUE_DUE_SLOW_COMMITS_REMOTE, txClass));
   }

   @ManagedOperation(description = "Number of early aborts per tx class",
                     displayName = "No. of early aborts per tx class")
   public final long getNumEarlyAbortsForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_EARLY_ABORTS, txClass));
   }

   @ManagedOperation(description = "Cost of terminating a tx (debug only) per tx class",
                     displayName = "Cost of terminating a tx (debug only) per tx class")
   public final long getTerminationCostForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(TERMINATION_COST, txClass));
   }

   @ManagedOperation(description = "Average TO-GMU prepare Rtt minus avg replay time at cohorts for successful tx per tx class",
                     displayName = "Avg TO-GMU Prepare RTT (- avg replay) time per tx class")
   public final long getTOGMUPrepareRttMinusAvgValidationTimeForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(TO_GMU_PREPARE_COMMAND_RTT_MINUS_AVG, txClass));
   }

   @ManagedOperation(description = "Average lock local holding time of committed tx (in microseconds) per tx class",
                     displayName = "Avg lock local holding time of committed tx per tx class")
   public final long getAvgLocalSuccessfulLockHoldTimeForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(SUX_LOCK_HOLD_TIME, txClass));
   }

   @ManagedOperation(description = "Avg Conditional Waiting time experience by TO-GMU prepare command per tx class",
                     displayName = "Avg Conditional Waiting time experience by TO-GMU prepare command per tx class")
   public final long getTOPrepareWaitTimeForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(TO_GMU_PREPARE_COMMAND_AVG_WAIT_TIME, txClass));
   }

   @ManagedOperation(description = "Avg number of local data items in a remote prepareCommand to read-validate per tx class",
                     displayName = "Avg no. of items in a remote PrepareCommand to read per tx class")
   public final double getNumOwnedRdItemsInRemotePrepareForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(NUM_OWNED_RD_ITEMS_IN_REMOTE_PREPARE, txClass));
   }

   @ManagedOperation(description = "Number of tx dead in remote prepare per tx class",
                     displayName = "No. of tx dead in remote prepare per tx class")
   public final long getRemotelyDeadXactForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_REMOTELY_ABORTED, txClass));
   }

   @ManagedOperation(description = "Number of local Tx that waited due to pending Tx per tx class",
                     displayName = "No. of local Tx that waited due to pending Tx per tx class")
   public final long getNumLocalTxWaitingTimeInGMUQueueDuePendingTxForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_GMU_WAITING_IN_QUEUE_DUE_PENDING_LOCAL, txClass));
   }

   @ManagedOperation(description = "Average TO-GMU mean replay time at cohorts for successful tx per tx class",
                     displayName = "Avg TO-GMU mean replay time at cohorts for successful tx per tx class")
   public final long getTOGMUAvgSuccessfulValidationServiceTimeForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(TO_GMU_PREPARE_COMMAND_SERVICE_TIME, txClass));
   }

   @ManagedOperation(description = "Avg CPU demand to execute a local RollbackCommand without remote nodes per tx class",
                     displayName = "Avg CPU to execute a local RollbackCommand without remote nodes per tx class")
   public final long getLocalUpdateTxLocalRollbackServiceTimeForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(UPDATE_TX_LOCAL_LOCAL_ROLLBACK_S, txClass));
   }

   @ManagedOperation(description = "Number of tx dead while preparing locally per tx class",
                     displayName = "No. of tx dead while preparing locally per tx class")
   public final long getNumLocalPrepareAbortsForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_LOCALPREPARE_ABORTS, txClass));
   }

   @ManagedOperation(description = "Avg number of local items in a local prepareCommand per tx class",
                     displayName = "Avg no. or items in a local prepareCommand to write per tx class")
   public final double getNumOwnedRdItemsInLocalPrepareForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(NUM_OWNED_RD_ITEMS_IN_LOCAL_PREPARE, txClass));
   }

   @ManagedOperation(description = "Average aborted write transaction duration (in microseconds) per tx class",
                     displayName = "Avg Aborted Write Transaction Duration per tx class")
   public final long getAvgAbortedWriteTxDurationForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(WR_TX_ABORTED_EXECUTION_TIME, txClass));
   }

   @ManagedOperation(description = "Average lock local holding time of remotely aborted tx (in microseconds) per tx class",
                     displayName = "Avg lock local holding time of remotely aborted tx per tx class")
   public final long getAvgLocalRemoteAbortLockHoldTimeForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(REMOTE_ABORT_LOCK_HOLD_TIME, txClass));
   }

   @ManagedOperation(description = "Avg CPU demand to execute a local PrepareCommand per tx class",
                     displayName = "Avg CPU to execute a local PrepareCommand per tx class")
   public final long getLocalUpdateTxPrepareServiceTimeForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(UPDATE_TX_LOCAL_PREPARE_S, txClass));
   }

   @ManagedOperation(description = "Avg response time of the execution of the commitCommand of a remote tx per tx class",
                     displayName = "Avg response time of the CommitCommand execution for remote tx per tx class")
   public final long getRemoteUpdateTxCommitResponseTimeForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(UPDATE_TX_REMOTE_COMMIT_R, txClass));
   }

   @ManagedOperation(description = "Remote Local Contentions per tx class",
                     displayName = "Remote Local Contentions per tx class")
   public final long getNumRemoteLocalLockContentionsForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(REMOTE_LOCK_CONTENTION_TO_LOCAL, txClass));
   }

   @ManagedOperation(description = "Avg response time of the local part of a tx up to the prepare phase per tx class",
                     displayName = "Avg response time of a tx up to the prepare phase per tx class")
   public final long getLocalUpdateTxLocalResponseTimeForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(UPDATE_TX_LOCAL_R, txClass));
   }

   @ManagedOperation(description = "Average cost of a remote get per tx class",
                     displayName = "Remote get cost per tx class")
   public final long getRemoteGetResponseTimeForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(LOCAL_REMOTE_GET_R, txClass));
   }

   @ManagedOperation(description = "Avg CPU demand to perform the local execution of a read only tx per tx class",
                     displayName = "Avg CPU to perform the local execution of a read only tx per tx class")
   public final long getLocalReadOnlyTxLocalServiceTimeForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(READ_ONLY_TX_LOCAL_S, txClass));
   }

   @ManagedOperation(description = "Avg response time of the local execution of a read only tx per tx class",
                     displayName = "Avg response time of local execution of a read only tx per tx class")
   public final long getLocalReadOnlyTxLocalResponseTimeForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(READ_ONLY_TX_LOCAL_R, txClass));
   }

   @ManagedOperation(description = "The number of aborted tx due to timeout in readlock acquisition per tx class",
                     displayName = "No. of aborted tx due to timeout in readlock acquisition per tx class")
   public final long getNumAbortedTxDueToReadLockForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_READLOCK_FAILED_TIMEOUT, txClass));
   }

   @ManagedOperation(description = "Avg response time of a local PrepareCommand per tx class",
                     displayName = "Avg response time of a local PrepareCommand per tx class")
   public final long getLocalUpdateTxPrepareResponseTimeForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(UPDATE_TX_LOCAL_PREPARE_R, txClass));
   }

   @ManagedOperation(description = "Avg cpu time for a GMUClusteredGetCommand per tx class",
                     displayName = "Avg cpu time for a GMUClusteredGetCommand per tx class")
   public final long getGMUClusteredGetCommandServiceTimeForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(REMOTE_REMOTE_GET_S, txClass));
   }

   @ManagedOperation(description = "Number of update tx gone up to prepare phase per tx class",
                     displayName = "No. of update tx gone up to prepare phase per tx class")
   public final long getUpdateXactToPrepareForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_UPDATE_TX_GOT_TO_PREPARE, txClass));
   }

   @ManagedOperation(description = "Average local Tx Waiting time due to slow commits per tx class",
                     displayName = "Avg local Tx Waiting time due to slow commits per tx class")
   public final long getAvgLocalTxWaitingTimeInGMUQueueDueToSlowCommitsForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(GMU_WAITING_IN_QUEUE_DUE_SLOW_COMMITS_LOCAL, txClass));
   }

   @ManagedOperation(description = "Avg waiting time before serving a ClusteredGetCommand per tx class",
                     displayName = "Avg waiting time before serving a ClusteredGetCommand per tx class")
   public final long getWaitedTimeInRemoteCommitQueueForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(WAIT_TIME_IN_REMOTE_COMMIT_QUEUE, txClass));
   }

   @ManagedOperation(description = "Avg response time of a local CommitCommand per tx class",
                     displayName = "Avg response time of a local CommitCommand per tx class")
   public final long getLocalUpdateTxCommitResponseTimeForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(UPDATE_TX_LOCAL_COMMIT_R, txClass));
   }

   @ManagedOperation(description = "Avg CPU demand to serve a remote get per tx class",
                     displayName = "Avg CPU to serve a remote get per tx class")
   public final long getRemoteGetServiceTimeForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(LOCAL_REMOTE_GET_S, txClass));
   }

   @ManagedOperation(description = "Avg response time for a GMUClusteredGetCommand (no queue) per tx class",
                     displayName = "Avg response time for a remote get (no queue) per tx class")
   public final long getGMUClusteredGetCommandResponseTimeForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(REMOTE_REMOTE_GET_R, txClass));
   }

   @ManagedOperation(description = "Average TO-GMU validations that waited on a node per tx class",
                     displayName = "Avg TO-GMU validations that waited on a node per tx class")
   public final long getTONumValidationWaitedForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_TO_GMU_PREPARE_COMMAND_REMOTE_WAITED, txClass));
   }

   @ManagedOperation(description = "Number of local aborted tx per tx class",
                     displayName = "No. of local aborted tx per tx class")
   public final long getNumAbortedXactsForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_ABORTED_WR_TX, txClass));
   }

   @ManagedOperation(description = "Avg response time of the commit of a read only tx per tx class",
                     displayName = "Avg response time of the commit of a read only tx per tx class")
   public final long getLocalReadOnlyTxCommitResponseTimeForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(READ_ONLY_TX_COMMIT_R, txClass));
   }

   @ManagedOperation(description = "Remote nodes from which a read-only transaction reads from per tx class",
                     displayName = "Remote nodes read by update xacts per tx class")
   public final double getLocalReadOnlyTxRemoteNodesReadForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(NUM_REMOTE_NODES_READ_RO_TX, txClass));
   }

   @ManagedOperation(description = "Remote nodes from which an update transaction reads from per tx class",
                     displayName = "Remote nodes read by read-only xacts per tx class")
   public final double getLocalUpdateTxRemoteNodesReadForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(NUM_REMOTE_NODES_READ_WR_TX, txClass));
   }

   @ManagedAttribute(description = "Remote nodes from which a read-only transaction reads from",
                     displayName = "Remote nodes read by read-only xacts")
   public final double getLocalReadOnlyTxRemoteNodesRead() {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(NUM_REMOTE_NODES_READ_RO_TX, null));
   }

   @ManagedAttribute(description = "Remote nodes from which an update transaction reads from",
                     displayName = "Remote nodes read by update xacts")
   public final double getLocalUpdateTxRemoteNodesRead() {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(NUM_REMOTE_NODES_READ_WR_TX, null));
   }

   @ManagedAttribute(description = "Number of local Update xact committed",
                     displayName = "Num of local committed update xact")
   public final long getLocalUpdateCommittedXact() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_COMMITTED_WR_TX, null));
   }

   @ManagedAttribute(description = "Number of local ReadOnly xact committed",
                     displayName = "Num of local committed readonly xact")
   public final long getLocalReadOnlyCommittedXact() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_COMMITTED_RO_TX, null));
   }

   @ManagedOperation(description = "Number of local Update xact committed per class",
                     displayName = "Num of local committed update xact per class")
   public final long getLocalUpdateCommittedXactForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_COMMITTED_WR_TX, txClass));
   }

   @ManagedOperation(description = "Number of local ReadOnly xact committed per class",
                     displayName = "Num of local committed readonly xact per class")
   public final long getLocalReadOnlyCommittedXactForTxClass(@Parameter(name = "Transaction Class") String txClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_COMMITTED_RO_TX, txClass));
   }

   //NB: readOnly transactions are never aborted (RC, RR, GMU)
   private void handleRollbackCommand(TransactionStatistics transactionStatistics, long initTime, long initCpuTime, TxInvocationContext ctx) {
      ExposedStatistic stat, cpuStat, counter;
      if (ctx.isOriginLocal()) {
         if (transactionStatistics.isPrepareSent() || ctx.getCacheTransaction().wasPrepareSent()) {
            cpuStat = UPDATE_TX_LOCAL_REMOTE_ROLLBACK_S;
            stat = UPDATE_TX_LOCAL_REMOTE_ROLLBACK_R;
            counter = NUM_UPDATE_TX_LOCAL_REMOTE_ROLLBACK;
            transactionStatistics.incrementValue(NUM_REMOTELY_ABORTED);
         } else {
            if (transactionStatistics.stillLocalExecution()) {
               transactionStatistics.incrementValue(NUM_EARLY_ABORTS);
            } else {
               transactionStatistics.incrementValue(NUM_LOCALPREPARE_ABORTS);
            }
            cpuStat = UPDATE_TX_LOCAL_LOCAL_ROLLBACK_S;
            stat = UPDATE_TX_LOCAL_LOCAL_ROLLBACK_R;
            counter = NUM_UPDATE_TX_LOCAL_LOCAL_ROLLBACK;
         }
      } else {
         cpuStat = UPDATE_TX_REMOTE_ROLLBACK_S;
         stat = UPDATE_TX_REMOTE_ROLLBACK_R;
         counter = NUM_UPDATE_TX_REMOTE_ROLLBACK;
      }
      updateWallClockTime(transactionStatistics, stat, counter, initTime);
      if (TransactionsStatisticsRegistry.isSampleServiceTime())
         updateServiceTimeWithoutCounter(transactionStatistics, cpuStat, initCpuTime);
   }

   private void handleCommitCommand(TransactionStatistics transactionStatistics, long initTime, long initCpuTime, TxInvocationContext ctx) {
      ExposedStatistic stat, cpuStat, counter;
      //In TO some xacts can be marked as RO even though remote
      if (ctx.isOriginLocal() && transactionStatistics.isReadOnly()) {
         cpuStat = READ_ONLY_TX_COMMIT_S;
         counter = NUM_READ_ONLY_TX_COMMIT;
         stat = READ_ONLY_TX_COMMIT_R;
      }
      //This is valid both for local and remote. The registry will populate the right container
      else {
         if (ctx.isOriginLocal()) {
            cpuStat = UPDATE_TX_LOCAL_COMMIT_S;
            counter = NUM_UPDATE_TX_LOCAL_COMMIT;
            stat = UPDATE_TX_LOCAL_COMMIT_R;
         } else {
            cpuStat = UPDATE_TX_REMOTE_COMMIT_S;
            counter = NUM_UPDATE_TX_REMOTE_COMMIT;
            stat = UPDATE_TX_REMOTE_COMMIT_R;
         }
      }

      updateWallClockTime(transactionStatistics, stat, counter, initTime);
      if (TransactionsStatisticsRegistry.isSampleServiceTime())
         updateServiceTimeWithoutCounter(transactionStatistics, cpuStat, initCpuTime);
   }

   /**
    * Increases the service and responseTime; This is invoked *only* if the prepareCommand is executed correctly
    */
   private void handlePrepareCommand(TransactionStatistics transactionStatistics, long initTime, long initCpuTime, TxInvocationContext ctx, PrepareCommand command) {
      ExposedStatistic stat, cpuStat, counter;
      if (transactionStatistics.isReadOnly()) {
         stat = READ_ONLY_TX_PREPARE_R;
         cpuStat = READ_ONLY_TX_PREPARE_S;
         updateWallClockTimeWithoutCounter(transactionStatistics, stat, initTime);
         if (TransactionsStatisticsRegistry.isSampleServiceTime())
            updateServiceTimeWithoutCounter(transactionStatistics, cpuStat, initCpuTime);
      }
      //This is valid both for local and remote. The registry will populate the right container
      else {
         if (ctx.isOriginLocal()) {
            stat = UPDATE_TX_LOCAL_PREPARE_R;
            cpuStat = UPDATE_TX_LOCAL_PREPARE_S;

         } else {
            stat = UPDATE_TX_REMOTE_PREPARE_R;
            cpuStat = UPDATE_TX_REMOTE_PREPARE_S;
         }
         counter = NUM_UPDATE_TX_PREPARED;
         updateWallClockTime(transactionStatistics, stat, counter, initTime);
         if (TransactionsStatisticsRegistry.isSampleServiceTime())
            updateServiceTimeWithoutCounter(transactionStatistics, cpuStat, initCpuTime);
      }

      //Take stats relevant to the avg number of read and write data items in the message
      //NB: these stats are taken only for prepare that will turn into a commit
      transactionStatistics.addValue(NUM_OWNED_RD_ITEMS_IN_OK_PREPARE, localRd(command));
      transactionStatistics.addValue(NUM_OWNED_WR_ITEMS_IN_OK_PREPARE, localWr(command));
   }

   private int localWr(PrepareCommand command) {
      WriteCommand[] wrSet = command.getModifications();
      int localWr = 0;
      for (WriteCommand wr : wrSet) {
         for (Object k : wr.getAffectedKeys()) {
            if (!isRemote(k))
               localWr++;
         }
      }
      return localWr;
   }

   private int localRd(PrepareCommand command) {
      if (!(command instanceof GMUPrepareCommand))
         return 0;
      int localRd = 0;

      Object[] rdSet = ((GMUPrepareCommand) command).getReadSet();
      for (Object rd : rdSet) {
         if (!isRemote(rd))
            localRd++;
      }
      return localRd;
   }

   private void replace() {
      log.info("CustomStatsInterceptor Enabled!");
      ComponentRegistry componentRegistry = cache.getAdvancedCache().getComponentRegistry();
      this.wireConfiguration();
      GlobalComponentRegistry globalComponentRegistry = componentRegistry.getGlobalComponentRegistry();
      InboundInvocationHandlerWrapper invocationHandlerWrapper = rewireInvocationHandler(globalComponentRegistry);
      globalComponentRegistry.rewire();

      replaceFieldInTransport(componentRegistry, invocationHandlerWrapper);

      replaceRpcManager(componentRegistry);
      replaceLockManager(componentRegistry);
      componentRegistry.rewire();
   }

   private void wireConfiguration() {
      this.configuration = cache.getAdvancedCache().getCacheConfiguration();
   }

   private void replaceFieldInTransport(ComponentRegistry componentRegistry, InboundInvocationHandlerWrapper invocationHandlerWrapper) {
      JGroupsTransport t = (JGroupsTransport) componentRegistry.getComponent(Transport.class);
      CommandAwareRpcDispatcher card = t.getCommandAwareRpcDispatcher();
      try {
         Field f = card.getClass().getDeclaredField("inboundInvocationHandler");
         f.setAccessible(true);
         f.set(card, invocationHandlerWrapper);
      } catch (NoSuchFieldException e) {
         e.printStackTrace();
      } catch (IllegalAccessException e) {
         e.printStackTrace();
      }
   }

   private InboundInvocationHandlerWrapper rewireInvocationHandler(GlobalComponentRegistry globalComponentRegistry) {
      InboundInvocationHandler inboundHandler = globalComponentRegistry.getComponent(InboundInvocationHandler.class);
      InboundInvocationHandlerWrapper invocationHandlerWrapper = new InboundInvocationHandlerWrapper(inboundHandler,
                                                                                                     transactionTable);
      globalComponentRegistry.registerComponent(invocationHandlerWrapper, InboundInvocationHandler.class);
      return invocationHandlerWrapper;
   }

   private void replaceLockManager(ComponentRegistry componentRegistry) {
      LockManager lockManager = componentRegistry.getComponent(LockManager.class);
      // this.configuration.customStatsConfiguration().sampleServiceTimes());
      LockManagerWrapper lockManagerWrapper = new LockManagerWrapper(lockManager, StreamLibContainer.getOrCreateStreamLibContainer(cache), true);
      componentRegistry.registerComponent(lockManagerWrapper, LockManager.class);
   }

   private void replaceRpcManager(ComponentRegistry componentRegistry) {
      RpcManager rpcManager = componentRegistry.getComponent(RpcManager.class);
      RpcManagerWrapper rpcManagerWrapper = new RpcManagerWrapper(rpcManager);
      componentRegistry.registerComponent(rpcManagerWrapper, RpcManager.class);
      this.rpcManager = rpcManagerWrapper;
   }

   private TransactionStatistics initStatsIfNecessary(InvocationContext ctx) {
      if (ctx.isInTxScope()) {
         TransactionStatistics transactionStatistics = TransactionsStatisticsRegistry
               .initTransactionIfNecessary((TxInvocationContext) ctx);
         if (transactionStatistics == null) {
            throw new IllegalStateException("Transaction Statistics cannot be null with transactional context");
         }
         return transactionStatistics;
      }
      return null;
   }

   private void updateWallClockTime(TransactionStatistics transactionStatistics, ExposedStatistic duration, ExposedStatistic counter, long initTime) {
      transactionStatistics.addValue(duration, System.nanoTime() - initTime);
      transactionStatistics.incrementValue(counter);
   }

   private void updateWallClockTimeWithoutCounter(TransactionStatistics transactionStatistics, ExposedStatistic duration, long initTime) {
      transactionStatistics.addValue(duration, System.nanoTime() - initTime);
   }

   private void updateServiceTimeWithoutCounter(TransactionStatistics transactionStatistics, ExposedStatistic time, long initTime) {
      transactionStatistics.addValue(time, TransactionsStatisticsRegistry.getThreadCPUTime() - initTime);
   }

   private long handleLong(Long value) {
      return value == null ? 0 : value;
   }

   private double handleDouble(Double value) {
      return value == null ? 0 : value;
   }

   private boolean isRemote(Object key) {
      return distributionManager != null && !distributionManager.getLocality(key).isLocal();
   }

}
