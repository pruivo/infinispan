package org.infinispan.distribution.wrappers;

import org.infinispan.commands.SetClassCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.EntryFactory;
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
import org.infinispan.stats.TransactionsStatisticsRegistry;
import org.infinispan.stats.topK.StreamLibContainer;
import org.infinispan.stats.translations.ExposedStatistics.IspnStats;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.WriteSkewException;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.locks.DeadlockDetectedException;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.lang.reflect.Field;

/**
 * Massive hack for a noble cause!
 *
 * @author Mircea Markus <mircea.markus@jboss.com> (C) 2011 Red Hat Inc.
 * @author Diego Didona <didona@gsd.inesc-id.pt>
 * @author Pedro Ruivo
 * @since 5.2
 */
@MBean(objectName = "ExtendedStatistics", description = "Component that manages and exposes extended statistics " +
      "relevant to transactions.")
public abstract class CustomStatsInterceptor extends BaseCustomInterceptor {
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
      log.warn("Initializing the TransactionStatisticsRegistry");
      TransactionsStatisticsRegistry.init(this.configuration);
   }

   @Override
   public Object visitSetClassCommand(InvocationContext ctx, SetClassCommand command) throws Throwable {
      log.tracef("visitSetClassCommand invoked");
      if (ctx.isInTxScope()) {
         this.initStatsIfNecessary(ctx);
      }
      TransactionsStatisticsRegistry.setTransactionalClass(command.getTransactionalClass());
      //TransactionsStatisticsRegistry.putThreadClasses(Thread.currentThread().getId(), command.getTransactionalClass());
      //System.out.println(threadClasses.get(Thread.currentThread().getId()));
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      log.tracef("Visit Put Key Value command %s. Is it in transaction scope? %s. Is it local? %s", command,
                 ctx.isInTxScope(), ctx.isOriginLocal());
      Object ret;
      if (ctx.isInTxScope()) {
         this.initStatsIfNecessary(ctx);
         TransactionsStatisticsRegistry.setUpdateTransaction();
         long currTime = System.nanoTime();
         TransactionsStatisticsRegistry.incrementValue(IspnStats.NUM_PUT);
         TransactionsStatisticsRegistry.addNTBCValue(currTime);
         try {
            ret = invokeNextInterceptor(ctx, command);
         } catch (TimeoutException e) {
            if (ctx.isOriginLocal() && isLockTimeout(e)) {
               TransactionsStatisticsRegistry.incrementValue(IspnStats.NUM_LOCK_FAILED_TIMEOUT);
            }
            throw e;
         } catch (DeadlockDetectedException e) {
            if (ctx.isOriginLocal()) {
               TransactionsStatisticsRegistry.incrementValue(IspnStats.NUM_LOCK_FAILED_DEADLOCK);
            }
            throw e;
         } catch (WriteSkewException e) {
            if (ctx.isOriginLocal()) {
               TransactionsStatisticsRegistry.incrementValue(IspnStats.NUM_WRITE_SKEW);
            }
            throw e;
         }
         if (isRemote(command.getKey())) {
            TransactionsStatisticsRegistry.addValue(IspnStats.REMOTE_PUT_EXECUTION, System.nanoTime() - currTime);
            TransactionsStatisticsRegistry.incrementValue(IspnStats.NUM_REMOTE_PUT);
         }
         TransactionsStatisticsRegistry.setLastOpTimestamp(System.nanoTime());
         return ret;
      } else
         return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      log.tracef("Visit Get Key Value command %s. Is it in transaction scope? %s. Is it local? %s", command,
                 ctx.isInTxScope(), ctx.isOriginLocal());
      boolean isTx = ctx.isInTxScope();
      Object ret;
      if (isTx) {
         long currTimeForAllGetCommand = System.nanoTime();
         TransactionsStatisticsRegistry.addNTBCValue(currTimeForAllGetCommand);
         this.initStatsIfNecessary(ctx);
         long currTime = 0;
         boolean isRemoteKey = isRemote(command.getKey());
         if (isRemoteKey) {
            currTime = System.nanoTime();
         }

         ret = invokeNextInterceptor(ctx, command);
         long lastTimeOp = System.nanoTime();
         if (isRemoteKey) {
            TransactionsStatisticsRegistry.incrementValue(IspnStats.NUM_REMOTE_GET);
            TransactionsStatisticsRegistry.addValue(IspnStats.REMOTE_GET_EXECUTION, lastTimeOp - currTime);
         }
         TransactionsStatisticsRegistry.addValue(IspnStats.ALL_GET_EXECUTION, lastTimeOp - currTimeForAllGetCommand);
         TransactionsStatisticsRegistry.incrementValue(IspnStats.NUM_GET);
         TransactionsStatisticsRegistry.setLastOpTimestamp(lastTimeOp);
      } else {
         ret = invokeNextInterceptor(ctx, command);
      }
      return ret;
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      log.tracef("Visit Commit command %s. Is it local?. Transaction is %s", command,
                 ctx.isOriginLocal(), command.getGlobalTransaction());
      this.initStatsIfNecessary(ctx);
      long currTime = System.nanoTime();
      TransactionsStatisticsRegistry.addNTBCValue(currTime);
      Object ret = invokeNextInterceptor(ctx, command);
      updateTime(IspnStats.COMMIT_EXECUTION_TIME, IspnStats.NUM_COMMIT_COMMAND, currTime);
      TransactionsStatisticsRegistry.setTransactionOutcome(true);
      if (ctx.isOriginLocal()) {
         TransactionsStatisticsRegistry.terminateTransaction();
      }
      return ret;
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      log.tracef("Visit Prepare command %s. Is it local?. Transaction is %s", command,
                 ctx.isOriginLocal(), command.getGlobalTransaction());
      this.initStatsIfNecessary(ctx);
      TransactionsStatisticsRegistry.onPrepareCommand();
      if (command.hasModifications()) {
         TransactionsStatisticsRegistry.setUpdateTransaction();
      }

      boolean success = false;
      try {
         long currTime = System.nanoTime();
         Object ret = invokeNextInterceptor(ctx, command);
         updateTime(IspnStats.PREPARE_EXECUTION_TIME, IspnStats.NUM_PREPARE_COMMAND, currTime);
         success = true;
         return ret;
      } catch (TimeoutException e) {
         if (ctx.isOriginLocal() && isLockTimeout(e)) {
            TransactionsStatisticsRegistry.incrementValue(IspnStats.NUM_LOCK_FAILED_TIMEOUT);
         }
         throw e;
      } catch (DeadlockDetectedException e) {
         if (ctx.isOriginLocal()) {
            TransactionsStatisticsRegistry.incrementValue(IspnStats.NUM_LOCK_FAILED_DEADLOCK);
         }
         throw e;
      } catch (WriteSkewException e) {
         if (ctx.isOriginLocal()) {
            TransactionsStatisticsRegistry.incrementValue(IspnStats.NUM_WRITE_SKEW);
         }
         throw e;
      } finally {
         if (command.isOnePhaseCommit()) {
            TransactionsStatisticsRegistry.setTransactionOutcome(success);
            if (ctx.isOriginLocal()) {
               TransactionsStatisticsRegistry.terminateTransaction();
            }
         }
      }
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      log.tracef("Visit Rollback command %s. Is it local?. Transaction is %s", command,
                 ctx.isOriginLocal(), command.getGlobalTransaction());
      this.initStatsIfNecessary(ctx);
      long initRollbackTime = System.nanoTime();
      Object ret = invokeNextInterceptor(ctx, command);
      updateTime(IspnStats.ROLLBACK_EXECUTION_TIME, IspnStats.NUM_ROLLBACKS, initRollbackTime);
      TransactionsStatisticsRegistry.setTransactionOutcome(false);
      if (ctx.isOriginLocal()) {
         TransactionsStatisticsRegistry.terminateTransaction();
      }
      return ret;
   }

   @ManagedAttribute(description = "Average number of puts performed by a successful local transaction",
                     displayName = "Number of puts per successful local transaction")
   public long getAvgNumPutsBySuccessfulLocalTx() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.PUTS_PER_LOCAL_TX));
   }

   @ManagedAttribute(description = "Average Prepare Round-Trip Time duration (in microseconds)",
                     displayName = "Average Prepare RTT")
   public long getAvgPrepareRtt() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((IspnStats.RTT_PREPARE))));
   }

   @ManagedAttribute(description = "Average Commit Round-Trip Time duration (in microseconds)",
                     displayName = "Average Commit RTT")
   public long getAvgCommitRtt() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((IspnStats.RTT_COMMIT))));
   }

   @ManagedAttribute(description = "Average Remote Get Round-Trip Time duration (in microseconds)",
                     displayName = "Average Remote Get RTT")
   public long getAvgRemoteGetRtt() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((IspnStats.RTT_GET))));
   }

   @ManagedAttribute(description = "Average Rollback Round-Trip Time duration (in microseconds)",
                     displayName = "Average Rollback RTT")
   public long getAvgRollbackRtt() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((IspnStats.RTT_ROLLBACK))));
   }

   @ManagedAttribute(description = "Average asynchronous Prepare duration (in microseconds)",
                     displayName = "Average Prepare Async")
   public long getAvgPrepareAsync() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((IspnStats.ASYNC_PREPARE))));
   }

   @ManagedAttribute(description = "Average asynchronous Commit duration (in microseconds)",
                     displayName = "Average Commit Async")
   public long getAvgCommitAsync() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((IspnStats.ASYNC_COMMIT))));
   }

   @ManagedAttribute(description = "Average asynchronous Complete Notification duration (in microseconds)",
                     displayName = "Average Complete Notification Async")
   public long getAvgCompleteNotificationAsync() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((IspnStats.ASYNC_COMPLETE_NOTIFY))));
   }

   @ManagedAttribute(description = "Average asynchronous Rollback duration (in microseconds)",
                     displayName = "Average Rollback Async")
   public long getAvgRollbackAsync() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((IspnStats.ASYNC_ROLLBACK))));
   }

   @ManagedAttribute(description = "Average number of nodes in Commit destination set",
                     displayName = "Average Number of Nodes in Commit Destination Set")
   public long getAvgNumNodesCommit() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((IspnStats.NUM_NODES_COMMIT))));
   }

   //JMX exposed methods

   @ManagedAttribute(description = "Average number of nodes in Complete Notification destination set",
                     displayName = "Average Number of Nodes in Complete Notification Destination Set")
   public long getAvgNumNodesCompleteNotification() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((IspnStats.NUM_NODES_COMPLETE_NOTIFY))));
   }

   @ManagedAttribute(description = "Average number of nodes in Remote Get destination set",
                     displayName = "Average Number of Nodes in Remote Get Destination Set")
   public long getAvgNumNodesRemoteGet() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((IspnStats.NUM_NODES_GET))));
   }

   @ManagedAttribute(description = "Average number of nodes in Prepare destination set",
                     displayName = "Average Number of Nodes in Prepare Destination Set")
   public long getAvgNumNodesPrepare() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((IspnStats.NUM_NODES_PREPARE))));
   }

   @ManagedAttribute(description = "Average number of nodes in Rollback destination set",
                     displayName = "Average Number of Nodes in Rollback Destination Set")
   public long getAvgNumNodesRollback() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((IspnStats.NUM_NODES_ROLLBACK))));
   }

   @ManagedAttribute(description = "Application Contention Factor",
                     displayName = "Application Contention Factor")
   public double getApplicationContentionFactor() {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute((IspnStats.APPLICATION_CONTENTION_FACTOR)));
   }

   @Deprecated
   @ManagedAttribute(description = "Local Contention Probability",
                     displayName = "Local Conflict Probability")
   public double getLocalContentionProbability() {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute((IspnStats.LOCAL_CONTENTION_PROBABILITY)));
   }

   @Deprecated
   @ManagedAttribute(description = "Remote Contention Probability",
                     displayName = "Remote Conflict Probability")
   public double getRemoteContentionProbability() {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute((IspnStats.REMOTE_CONTENTION_PROBABILITY)));
   }

   @ManagedAttribute(description = "Lock Contention Probability",
                     displayName = "Lock Contention Probability")
   public double getLockContentionProbability() {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute((IspnStats.LOCK_CONTENTION_PROBABILITY)));
   }

   @ManagedAttribute(description = "Local execution time of a transaction without the time waiting for lock acquisition",
                     displayName = "Local Execution Time Without Locking Time")
   public long getLocalExecutionTimeWithoutLock() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.LOCAL_EXEC_NO_CONT));
   }

   @ManagedAttribute(description = "Average lock holding time (in microseconds)",
                     displayName = "Average Lock Holding Time")
   public long getAvgLockHoldTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.LOCK_HOLD_TIME));
   }

   @ManagedAttribute(description = "Average lock local holding time (in microseconds)",
                     displayName = "Average Lock Local Holding Time")
   public long getAvgLocalLockHoldTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.LOCK_HOLD_TIME_LOCAL));
   }

   @ManagedAttribute(description = "Average lock remote holding time (in microseconds)",
                     displayName = "Average Lock Remote Holding Time")
   public long getAvgRemoteLockHoldTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.LOCK_HOLD_TIME_REMOTE));
   }

   @ManagedAttribute(description = "Average local commit duration time (2nd phase only) (in microseconds)",
                     displayName = "Average Commit Time")
   public long getAvgCommitTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.COMMIT_EXECUTION_TIME));
   }

   @ManagedAttribute(description = "Average local rollback duration time (2nd phase only) (in microseconds)",
                     displayName = "Average Rollback Time")
   public long getAvgRollbackTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.ROLLBACK_EXECUTION_TIME));
   }

   @ManagedAttribute(description = "Average prepare command size (in bytes)",
                     displayName = "Average Prepare Command Size")
   public long getAvgPrepareCommandSize() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.PREPARE_COMMAND_SIZE));
   }

   @ManagedAttribute(description = "Average commit command size (in bytes)",
                     displayName = "Average Commit Command Size")
   public long getAvgCommitCommandSize() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.COMMIT_COMMAND_SIZE));
   }

   @ManagedAttribute(description = "Average clustered get command size (in bytes)",
                     displayName = "Average Clustered Get Command Size")
   public long getAvgClusteredGetCommandSize() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.CLUSTERED_GET_COMMAND_SIZE));
   }

   @ManagedAttribute(description = "Average time waiting for the lock acquisition (in microseconds)",
                     displayName = "Average Lock Waiting Time")
   public long getAvgLockWaitingTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.LOCK_WAITING_TIME));
   }

   @ManagedAttribute(description = "Average transaction arrival rate, originated locally and remotely (in transaction " +
         "per second)",
                     displayName = "Average Transaction Arrival Rate")
   public double getAvgTxArrivalRate() {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(IspnStats.ARRIVAL_RATE));
   }

   @ManagedAttribute(description = "Percentage of Write transaction executed locally (committed and aborted)",
                     displayName = "Percentage of Write Transactions")
   public double getPercentageWriteTransactions() {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(IspnStats.TX_WRITE_PERCENTAGE));
   }

   @ManagedAttribute(description = "Percentage of Write transaction executed in all successfully executed " +
         "transactions (local transaction only)",
                     displayName = "Percentage of Successfully Write Transactions")
   public double getPercentageSuccessWriteTransactions() {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(IspnStats.SUCCESSFUL_WRITE_PERCENTAGE));
   }

   @ManagedAttribute(description = "The number of aborted transactions due to timeout in lock acquisition",
                     displayName = "Number of Aborted Transaction due to Lock Acquisition Timeout")
   public long getNumAbortedTxDueTimeout() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_LOCK_FAILED_TIMEOUT));
   }

   @ManagedAttribute(description = "The number of aborted transactions due to deadlock",
                     displayName = "Number of Aborted Transaction due to Deadlock")
   public long getNumAbortedTxDueDeadlock() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_LOCK_FAILED_DEADLOCK));
   }

   @ManagedAttribute(description = "Average successful read-only transaction duration (in microseconds)",
                     displayName = "Average Read-Only Transaction Duration")
   public long getAvgReadOnlyTxDuration() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.RO_TX_SUCCESSFUL_EXECUTION_TIME));
   }

   @ManagedAttribute(description = "Average successful write transaction duration (in microseconds)",
                     displayName = "Average Write Transaction Duration")
   public long getAvgWriteTxDuration() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.WR_TX_SUCCESSFUL_EXECUTION_TIME));
   }

   @ManagedAttribute(description = "Average aborted write transaction duration (in microseconds)",
                     displayName = "Average Aborted Write Transaction Duration")
   public long getAvgAbortedWriteTxDuration() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.WR_TX_ABORTED_EXECUTION_TIME));
   }

   @ManagedAttribute(description = "Average write transaction local execution time (in microseconds)",
                     displayName = "Average Write Transaction Local Execution Time")
   public long getAvgWriteTxLocalExecution() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.WR_TX_LOCAL_EXECUTION_TIME));
   }

   @ManagedAttribute(description = "Average number of locks per write local transaction",
                     displayName = "Average Number of Lock per Local Transaction")
   public long getAvgNumOfLockLocalTx() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_LOCK_PER_LOCAL_TX));
   }

   @ManagedAttribute(description = "Average number of locks per write remote transaction",
                     displayName = "Average Number of Lock per Remote Transaction")
   public long getAvgNumOfLockRemoteTx() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_LOCK_PER_REMOTE_TX));
   }

   @ManagedAttribute(description = "Average number of locks per successfully write local transaction",
                     displayName = "Average Number of Lock per Successfully Local Transaction")
   public long getAvgNumOfLockSuccessLocalTx() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_LOCK_PER_SUCCESS_LOCAL_TX));
   }

   @ManagedAttribute(description = "Average time it takes to execute the prepare command locally (in microseconds)",
                     displayName = "Average Local Prepare Execution Time")
   public long getAvgLocalPrepareTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.LOCAL_PREPARE_EXECUTION_TIME));
   }

   @ManagedAttribute(description = "Average time it takes to execute the prepare command remotely (in microseconds)",
                     displayName = "Average Remote Prepare Execution Time")
   public long getAvgRemotePrepareTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.REMOTE_PREPARE_EXECUTION_TIME));
   }

   @ManagedAttribute(description = "Average time it takes to execute the commit command locally (in microseconds)",
                     displayName = "Average Local Commit Execution Time")
   public long getAvgLocalCommitTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.LOCAL_COMMIT_EXECUTION_TIME));
   }

   @ManagedAttribute(description = "Average time it takes to execute the commit command remotely (in microseconds)",
                     displayName = "Average Remote Commit Execution Time")
   public long getAvgRemoteCommitTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.REMOTE_COMMIT_EXECUTION_TIME));
   }

   @ManagedAttribute(description = "Average time it takes to execute the rollback command locally (in microseconds)",
                     displayName = "Average Local Rollback Execution Time")
   public long getAvgLocalRollbackTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.LOCAL_ROLLBACK_EXECUTION_TIME));
   }

   @ManagedAttribute(description = "Average time it takes to execute the rollback command remotely (in microseconds)",
                     displayName = "Average Remote Rollback Execution Time")
   public long getAvgRemoteRollbackTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.REMOTE_ROLLBACK_EXECUTION_TIME));
   }

   @ManagedAttribute(description = "Average time it takes to execute the rollback command remotely (in microseconds)",
                     displayName = "Average Remote Transaction Completion Notify Execution Time")
   public long getAvgRemoteTxCompleteNotifyTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.TX_COMPLETE_NOTIFY_EXECUTION_TIME));
   }

   @ManagedAttribute(description = "Abort Rate",
                     displayName = "Abort Rate")
   public double getAbortRate() {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(IspnStats.ABORT_RATE));
   }

   @ManagedAttribute(description = "Throughput (in transactions per second)",
                     displayName = "Throughput")
   public double getThroughput() {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(IspnStats.THROUGHPUT));
   }

   @ManagedAttribute(description = "Average number of get operations per local read transaction",
                     displayName = "Average number of get operations per local read transaction")
   public long getAvgGetsPerROTransaction() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_SUCCESSFUL_GETS_RO_TX));
   }

   @ManagedAttribute(description = "Average number of get operations per local write transaction",
                     displayName = "Average number of get operations per local write transaction")
   public long getAvgGetsPerWrTransaction() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_SUCCESSFUL_GETS_WR_TX));
   }

   @ManagedAttribute(description = "Average number of remote get operations per local write transaction",
                     displayName = "Average number of remote get operations per local write transaction")
   public long getAvgRemoteGetsPerWrTransaction() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_SUCCESSFUL_REMOTE_GETS_WR_TX));
   }

   @ManagedAttribute(description = "Average number of remote get operations per local read transaction",
                     displayName = "Average number of remote get operations per local read transaction")
   public long getAvgRemoteGetsPerROTransaction() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_SUCCESSFUL_REMOTE_GETS_RO_TX));
   }

   @ManagedAttribute(description = "Average cost of a remote get",
                     displayName = "Remote get cost")
   public long getRemoteGetExecutionTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.REMOTE_GET_EXECUTION));
   }

   @ManagedAttribute(description = "Average number of put operations per local write transaction",
                     displayName = "Average number of put operations per local write transaction")
   public long getAvgPutsPerWrTransaction() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_SUCCESSFUL_PUTS_WR_TX));
   }

   @ManagedAttribute(description = "Average number of remote put operations per local write transaction",
                     displayName = "Average number of remote put operations per local write transaction")
   public long getAvgRemotePutsPerWrTransaction() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_SUCCESSFUL_REMOTE_PUTS_WR_TX));
   }

   @ManagedAttribute(description = "Average cost of a remote put",
                     displayName = "Remote put cost")
   public long getRemotePutExecutionTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.REMOTE_PUT_EXECUTION));
   }

   @ManagedAttribute(description = "Number of gets performed since last reset",
                     displayName = "Number of Gets")
   public long getNumberOfGets() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_GET));
   }

   @ManagedAttribute(description = "Number of remote gets performed since last reset",
                     displayName = "Number of Remote Gets")
   public long getNumberOfRemoteGets() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_REMOTE_GET));
   }

   @ManagedAttribute(description = "Number of puts performed since last reset",
                     displayName = "Number of Puts")
   public long getNumberOfPuts() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_PUT));
   }

   @ManagedAttribute(description = "Number of remote puts performed since last reset",
                     displayName = "Number of Remote Puts")
   public long getNumberOfRemotePuts() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_REMOTE_PUT));
   }

   @ManagedAttribute(description = "Number of committed transactions since last reset",
                     displayName = "Number Of Commits")
   public long getNumberOfCommits() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_COMMITS));
   }

   @ManagedAttribute(description = "Number of local committed transactions since last reset",
                     displayName = "Number Of Local Commits")
   public long getNumberOfLocalCommits() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_LOCAL_COMMITS));
   }

   @ManagedAttribute(description = "Write skew probability",
                     displayName = "Write Skew Probability")
   public double getWriteSkewProbability() {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(IspnStats.WRITE_SKEW_PROBABILITY));
   }

   @ManagedOperation(description = "K-th percentile of local read-only transactions execution time",
                     displayName = "K-th Percentile Local Read-Only Transactions")
   public double getPercentileLocalReadOnlyTransaction(@Parameter(name = "Percentile") int percentile) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getPercentile(IspnStats.RO_LOCAL_PERCENTILE, percentile));
   }

   @ManagedOperation(description = "K-th percentile of remote read-only transactions execution time",
                     displayName = "K-th Percentile Remote Read-Only Transactions")
   public double getPercentileRemoteReadOnlyTransaction(@Parameter(name = "Percentile") int percentile) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getPercentile(IspnStats.RO_REMOTE_PERCENTILE, percentile));
   }

   @ManagedOperation(description = "K-th percentile of local write transactions execution time",
                     displayName = "K-th Percentile Local Write Transactions")
   public double getPercentileLocalRWriteTransaction(@Parameter(name = "Percentile") int percentile) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getPercentile(IspnStats.WR_LOCAL_PERCENTILE, percentile));
   }

   @ManagedOperation(description = "K-th percentile of remote write transactions execution time",
                     displayName = "K-th Percentile Remote Write Transactions")
   public double getPercentileRemoteWriteTransaction(@Parameter(name = "Percentile") int percentile) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getPercentile(IspnStats.WR_REMOTE_PERCENTILE, percentile));
   }

   @ManagedOperation(description = "Reset all the statistics collected",
                     displayName = "Reset All Statistics")
   public void resetStatistics() {
      TransactionsStatisticsRegistry.reset();
   }

   @ManagedAttribute(description = "Average Local processing Get time (in microseconds)",
                     displayName = "Average Local Get time")
   public long getAvgLocalGetTime() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((IspnStats.LOCAL_GET_EXECUTION))));
   }

   @ManagedAttribute(description = "Average TCB time (in microseconds)",
                     displayName = "Average TCB time")
   public long getAvgTCBTime() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((IspnStats.TBC))));
   }

   @ManagedAttribute(description = "Average NTCB time (in microseconds)",
                     displayName = "Average NTCB time")
   public long getAvgNTCBTime() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((IspnStats.NTBC))));
   }

   @ManagedAttribute(description = "Number of nodes in the cluster",
                     displayName = "Number of nodes")
   public long getNumNodes() {
      return configuration.clustering().cacheMode().isClustered() ? rpcManager.getMembers().size() : 1;
   }

   @ManagedAttribute(description = "Number of replicas for each key",
                     displayName = "Replication Degree")
   public long getReplicationDegree() {
      return configuration.clustering().cacheMode().isClustered() ? rpcManager.getMembers().size() :
            distributionManager == null ? 1 : distributionManager.getConsistentHash().getNumOwners();
   }

   @ManagedAttribute(description = "Number of concurrent transactions executing on the current node",
                     displayName = "Local Active Transactions")
   public long getLocalActiveTransactions() {
      return transactionTable == null ? 0 : transactionTable.getLocalTxCount();
   }

   @ManagedAttribute(description = "Average Response Time",
                     displayName = "Average Response Time")
   public long getAvgResponseTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.RESPONSE_TIME));
   }

   @ManagedOperation(description = "Average number of puts performed by a successful local transaction per class",
                     displayName = "Number of puts per successful local transaction per class")
   public long getAvgNumPutsBySuccessfulLocalTxParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.PUTS_PER_LOCAL_TX, transactionalClass));
   }

   @ManagedOperation(description = "Average Prepare Round-Trip Time duration (in microseconds) per class",
                     displayName = "Average Prepare RTT per class")
   public long getAvgPrepareRttParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((IspnStats.RTT_PREPARE), transactionalClass)));
   }

   @ManagedOperation(description = "Average Commit Round-Trip Time duration (in microseconds) per class",
                     displayName = "Average Commit RTT per class")
   public long getAvgCommitRttParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((IspnStats.RTT_COMMIT), transactionalClass)));
   }

   @ManagedOperation(description = "Average Remote Get Round-Trip Time duration (in microseconds) per class",
                     displayName = "Average Remote Get RTT per class")
   public long getAvgRemoteGetRttParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((IspnStats.RTT_GET), transactionalClass)));
   }

   @ManagedOperation(description = "Average Rollback Round-Trip Time duration (in microseconds) per class",
                     displayName = "Average Rollback RTT per class")
   public long getAvgRollbackRttParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((IspnStats.RTT_ROLLBACK), transactionalClass)));
   }

   @ManagedOperation(description = "Average asynchronous Prepare duration (in microseconds) per class",
                     displayName = "Average Prepare Async per class")
   public long getAvgPrepareAsyncParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((IspnStats.ASYNC_PREPARE), transactionalClass)));
   }

   @ManagedOperation(description = "Average asynchronous Commit duration (in microseconds) per class",
                     displayName = "Average Commit Async per class")
   public long getAvgCommitAsyncParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((IspnStats.ASYNC_COMMIT), transactionalClass)));
   }

   @ManagedOperation(description = "Average asynchronous Complete Notification duration (in microseconds) per class",
                     displayName = "Average Complete Notification Async per class")
   public long getAvgCompleteNotificationAsyncParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((IspnStats.ASYNC_COMPLETE_NOTIFY), transactionalClass)));
   }

   @ManagedOperation(description = "Average asynchronous Rollback duration (in microseconds) per class",
                     displayName = "Average Rollback Async per class")
   public long getAvgRollbackAsyncParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((IspnStats.ASYNC_ROLLBACK), transactionalClass)));
   }

   @ManagedOperation(description = "Average number of nodes in Commit destination set per class",
                     displayName = "Average Number of Nodes in Commit Destination Set per class")
   public long getAvgNumNodesCommitParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((IspnStats.NUM_NODES_COMMIT), transactionalClass)));
   }

   @ManagedOperation(description = "Average number of nodes in Complete Notification destination set per class",
                     displayName = "Average Number of Nodes in Complete Notification Destination Set per class")
   public long getAvgNumNodesCompleteNotificationParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((IspnStats.NUM_NODES_COMPLETE_NOTIFY), transactionalClass)));
   }

   @ManagedOperation(description = "Average number of nodes in Remote Get destination set per class",
                     displayName = "Average Number of Nodes in Remote Get Destination Set per class")
   public long getAvgNumNodesRemoteGetParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((IspnStats.NUM_NODES_GET), transactionalClass)));
   }

   //JMX with Transactional class xxxxxxxxx

   @ManagedOperation(description = "Average number of nodes in Prepare destination set per class",
                     displayName = "Average Number of Nodes in Prepare Destination Set per class")
   public long getAvgNumNodesPrepareParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((IspnStats.NUM_NODES_PREPARE), transactionalClass)));
   }

   @ManagedOperation(description = "Average number of nodes in Rollback destination set per class",
                     displayName = "Average Number of Nodes in Rollback Destination Set per class")
   public long getAvgNumNodesRollbackParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((IspnStats.NUM_NODES_ROLLBACK), transactionalClass)));
   }

   @ManagedOperation(description = "Application Contention Factor per class",
                     displayName = "Application Contention Factor per class")
   public double getApplicationContentionFactorParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute((IspnStats.APPLICATION_CONTENTION_FACTOR), transactionalClass));
   }

   @Deprecated
   @ManagedOperation(description = "Local Contention Probability per class",
                     displayName = "Local Conflict Probability per class")
   public double getLocalContentionProbabilityParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute((IspnStats.LOCAL_CONTENTION_PROBABILITY), transactionalClass));
   }

   @ManagedOperation(description = "Remote Contention Probability per class",
                     displayName = "Remote Conflict Probability per class")
   public double getRemoteContentionProbabilityParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute((IspnStats.REMOTE_CONTENTION_PROBABILITY), transactionalClass));
   }

   @ManagedOperation(description = "Lock Contention Probability per class",
                     displayName = "Lock Contention Probability per class")
   public double getLockContentionProbabilityParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute((IspnStats.LOCK_CONTENTION_PROBABILITY), transactionalClass));
   }

   @ManagedOperation(description = "Local execution time of a transaction without the time waiting for lock acquisition per class",
                     displayName = "Local Execution Time Without Locking Time per class")
   public long getLocalExecutionTimeWithoutLockParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.LOCAL_EXEC_NO_CONT, transactionalClass));
   }

   @ManagedOperation(description = "Average lock holding time (in microseconds) per class",
                     displayName = "Average Lock Holding Time per class")
   public long getAvgLockHoldTimeParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.LOCK_HOLD_TIME, transactionalClass));
   }

   @ManagedOperation(description = "Average lock local holding time (in microseconds) per class",
                     displayName = "Average Lock Local Holding Time per class")
   public long getAvgLocalLockHoldTimeParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.LOCK_HOLD_TIME_LOCAL, transactionalClass));
   }

   @ManagedOperation(description = "Average lock remote holding time (in microseconds) per class",
                     displayName = "Average Lock Remote Holding Time per class")
   public long getAvgRemoteLockHoldTimeParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.LOCK_HOLD_TIME_REMOTE, transactionalClass));
   }

   @ManagedOperation(description = "Average local commit duration time (2nd phase only) (in microseconds) per class",
                     displayName = "Average Commit Time per class")
   public long getAvgCommitTimeParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.COMMIT_EXECUTION_TIME, transactionalClass));
   }

   @ManagedOperation(description = "Average local rollback duration time (2nd phase only) (in microseconds) per class",
                     displayName = "Average Rollback Time per class")
   public long getAvgRollbackTimeParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.ROLLBACK_EXECUTION_TIME, transactionalClass));
   }

   @ManagedOperation(description = "Average prepare command size (in bytes) per class",
                     displayName = "Average Prepare Command Size per class")
   public long getAvgPrepareCommandSizeParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.PREPARE_COMMAND_SIZE, transactionalClass));
   }

   @ManagedOperation(description = "Average commit command size (in bytes) per class",
                     displayName = "Average Commit Command Size per class")
   public long getAvgCommitCommandSizeParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.COMMIT_COMMAND_SIZE, transactionalClass));
   }

   @ManagedOperation(description = "Average clustered get command size (in bytes) per class",
                     displayName = "Average Clustered Get Command Size per class")
   public long getAvgClusteredGetCommandSizeParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.CLUSTERED_GET_COMMAND_SIZE, transactionalClass));
   }

   @ManagedOperation(description = "Average time waiting for the lock acquisition (in microseconds) per class",
                     displayName = "Average Lock Waiting Time per class")
   public long getAvgLockWaitingTimeParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.LOCK_WAITING_TIME, transactionalClass));
   }

   @ManagedOperation(description = "Average transaction arrival rate, originated locally and remotely (in transaction " +
         "per second) per class",
                     displayName = "Average Transaction Arrival Rate per class")
   public double getAvgTxArrivalRateParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(IspnStats.ARRIVAL_RATE, transactionalClass));
   }

   @ManagedOperation(description = "Percentage of Write transaction executed locally (committed and aborted) per class",
                     displayName = "Percentage of Write Transactions per class")
   public double getPercentageWriteTransactionsParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(IspnStats.TX_WRITE_PERCENTAGE, transactionalClass));
   }

   @ManagedOperation(description = "Percentage of Write transaction executed in all successfully executed " +
         "transactions (local transaction only) per class",
                     displayName = "Percentage of Successfully Write Transactions per class")
   public double getPercentageSuccessWriteTransactionsParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(IspnStats.SUCCESSFUL_WRITE_PERCENTAGE, transactionalClass));
   }

   @ManagedOperation(description = "The number of aborted transactions due to timeout in lock acquisition per class",
                     displayName = "Number of Aborted Transaction due to Lock Acquisition Timeout per class")
   public long getNumAbortedTxDueTimeoutParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_LOCK_FAILED_TIMEOUT, transactionalClass));
   }

   @ManagedOperation(description = "The number of aborted transactions due to deadlock per class",
                     displayName = "Number of Aborted Transaction due to Deadlock per class")
   public long getNumAbortedTxDueDeadlockParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_LOCK_FAILED_DEADLOCK, transactionalClass));
   }

   @ManagedOperation(description = "Average successful read-only transaction duration (in microseconds) per class",
                     displayName = "Average Read-Only Transaction Duration per class")
   public long getAvgReadOnlyTxDurationParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.RO_TX_SUCCESSFUL_EXECUTION_TIME, transactionalClass));
   }

   @ManagedOperation(description = "Average successful write transaction duration (in microseconds) per class",
                     displayName = "Average Write Transaction Duration per class")
   public long getAvgWriteTxDurationParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.WR_TX_SUCCESSFUL_EXECUTION_TIME, transactionalClass));
   }

   @ManagedOperation(description = "Average write transaction local execution time (in microseconds) per class",
                     displayName = "Average Write Transaction Local Execution Time per class")
   public long getAvgWriteTxLocalExecutionParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.WR_TX_LOCAL_EXECUTION_TIME, transactionalClass));
   }

   @ManagedOperation(description = "Average number of locks per write local transaction per class",
                     displayName = "Average Number of Lock per Local Transaction per class")
   public long getAvgNumOfLockLocalTxParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_LOCK_PER_LOCAL_TX, transactionalClass));
   }

   @ManagedOperation(description = "Average number of locks per write remote transaction per class",
                     displayName = "Average Number of Lock per Remote Transaction per class")
   public long getAvgNumOfLockRemoteTxParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_LOCK_PER_REMOTE_TX, transactionalClass));
   }

   @ManagedOperation(description = "Average number of locks per successfully write local transaction per class",
                     displayName = "Average Number of Lock per Successfully Local Transaction per class")
   public long getAvgNumOfLockSuccessLocalTxParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_LOCK_PER_SUCCESS_LOCAL_TX, transactionalClass));
   }

   @ManagedOperation(description = "Average time it takes to execute the prepare command locally (in microseconds) per class",
                     displayName = "Average Local Prepare Execution Time per class")
   public long getAvgLocalPrepareTimeParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.LOCAL_PREPARE_EXECUTION_TIME, transactionalClass));
   }

   @ManagedOperation(description = "Average time it takes to execute the prepare command remotely (in microseconds) per class",
                     displayName = "Average Remote Prepare Execution Time per class")
   public long getAvgRemotePrepareTimeParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.REMOTE_PREPARE_EXECUTION_TIME, transactionalClass));
   }

   @ManagedOperation(description = "Average time it takes to execute the commit command locally (in microseconds) per class",
                     displayName = "Average Local Commit Execution Time per class")
   public long getAvgLocalCommitTimeParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.LOCAL_COMMIT_EXECUTION_TIME, transactionalClass));
   }

   @ManagedOperation(description = "Average time it takes to execute the commit command remotely (in microseconds) per class",
                     displayName = "Average Remote Commit Execution Time per class")
   public long getAvgRemoteCommitTimeParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.REMOTE_COMMIT_EXECUTION_TIME, transactionalClass));
   }

   @ManagedOperation(description = "Average time it takes to execute the rollback command locally (in microseconds) per class",
                     displayName = "Average Local Rollback Execution Time per class")
   public long getAvgLocalRollbackTimeParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.LOCAL_ROLLBACK_EXECUTION_TIME, transactionalClass));
   }

   @ManagedOperation(description = "Average time it takes to execute the rollback command remotely (in microseconds) per class",
                     displayName = "Average Remote Rollback Execution Time per class")
   public long getAvgRemoteRollbackTimeParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.REMOTE_ROLLBACK_EXECUTION_TIME, transactionalClass));
   }

   @ManagedOperation(description = "Average time it takes to execute the rollback command remotely (in microseconds) per class",
                     displayName = "Average Remote Transaction Completion Notify Execution Time per class")
   public long getAvgRemoteTxCompleteNotifyTimeParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.TX_COMPLETE_NOTIFY_EXECUTION_TIME, transactionalClass));
   }

   @ManagedOperation(description = "Abort Rate per class",
                     displayName = "Abort Rate per class")
   public double getAbortRateParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(IspnStats.ABORT_RATE, transactionalClass));
   }

   @ManagedOperation(description = "Throughput (in transactions per second) per class",
                     displayName = "Throughput per class")
   public double getThroughputParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(IspnStats.THROUGHPUT, transactionalClass));
   }

   @ManagedOperation(description = "Average number of get operations per local read transaction per class",
                     displayName = "Average number of get operations per local read transaction per class")
   public long getAvgGetsPerROTransactionParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_SUCCESSFUL_GETS_RO_TX, transactionalClass));
   }

   @ManagedOperation(description = "Average number of get operations per local write transaction per class",
                     displayName = "Average number of get operations per local write transaction per class")
   public long getAvgGetsPerWrTransactionParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_SUCCESSFUL_GETS_WR_TX, transactionalClass));
   }

   @ManagedOperation(description = "Average number of remote get operations per local write transaction per class",
                     displayName = "Average number of remote get operations per local write transaction per class")
   public long getAvgRemoteGetsPerWrTransactionParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_SUCCESSFUL_REMOTE_GETS_WR_TX, transactionalClass));
   }

   @ManagedOperation(description = "Average number of remote get operations per local read transaction per class",
                     displayName = "Average number of remote get operations per local read transaction per class")
   public long getAvgRemoteGetsPerROTransactionParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_SUCCESSFUL_REMOTE_GETS_RO_TX, transactionalClass));
   }

   @ManagedOperation(description = "Average cost of a remote get per class",
                     displayName = "Remote get cost per class")
   public long getRemoteGetExecutionTimeParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.REMOTE_GET_EXECUTION, transactionalClass));
   }

   @ManagedOperation(description = "Average number of put operations per local write transaction per class",
                     displayName = "Average number of put operations per local write transaction per class")
   public long getAvgPutsPerWrTransactionParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_SUCCESSFUL_PUTS_WR_TX, transactionalClass));
   }

   @ManagedOperation(description = "Average number of remote put operations per local write transaction per class",
                     displayName = "Average number of remote put operations per local write transaction per class")
   public long getAvgRemotePutsPerWrTransactionParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_SUCCESSFUL_REMOTE_PUTS_WR_TX, transactionalClass));
   }

   @ManagedOperation(description = "Average cost of a remote put per class",
                     displayName = "Remote put cost per class")
   public long getRemotePutExecutionTimeParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.REMOTE_PUT_EXECUTION, transactionalClass));
   }

   @ManagedOperation(description = "Number of gets performed since last reset per class",
                     displayName = "Number of Gets per class")
   public long getNumberOfGetsParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_GET, transactionalClass));
   }

   @ManagedOperation(description = "Number of remote gets performed since last reset per class",
                     displayName = "Number of Remote Gets per class")
   public long getNumberOfRemoteGetsParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_REMOTE_GET, transactionalClass));
   }

   @ManagedOperation(description = "Number of puts performed since last reset per class",
                     displayName = "Number of Puts per class")
   public long getNumberOfPutsParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_PUT, transactionalClass));
   }

   @ManagedOperation(description = "Number of remote puts performed since last reset per class",
                     displayName = "Number of Remote Puts per class")
   public long getNumberOfRemotePutsParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_REMOTE_PUT, transactionalClass));
   }

   @ManagedOperation(description = "Number of committed transactions since last reset per class",
                     displayName = "Number Of Commits per class")
   public long getNumberOfCommitsParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_COMMITS, transactionalClass));
   }

   @ManagedOperation(description = "Number of local committed transactions since last reset per class",
                     displayName = "Number Of Local Commits per class")
   public long getNumberOfLocalCommitsParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_LOCAL_COMMITS, transactionalClass));
   }

   @ManagedOperation(description = "Write skew probability per class",
                     displayName = "Write Skew Probability per class")
   public double getWriteSkewProbabilityParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(IspnStats.WRITE_SKEW_PROBABILITY, transactionalClass));
   }

   @ManagedOperation(description = "K-th percentile of local read-only transactions execution time per class",
                     displayName = "K-th Percentile Local Read-Only Transactions per class")
   public double getPercentileLocalReadOnlyTransactionParam(@Parameter(name = "Percentile") int percentile, @Parameter(name = "Transaction Class") String transactionalClass) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getPercentile(IspnStats.RO_LOCAL_PERCENTILE, percentile, transactionalClass));
   }

   @ManagedOperation(description = "K-th percentile of remote read-only transactions execution time per class",
                     displayName = "K-th Percentile Remote Read-Only Transactions per class")
   public double getPercentileRemoteReadOnlyTransactionParam(@Parameter(name = "Percentile") int percentile, @Parameter(name = "Transaction Class") String transactionalClass) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getPercentile(IspnStats.RO_REMOTE_PERCENTILE, percentile, transactionalClass));
   }

   @ManagedOperation(description = "K-th percentile of local write transactions execution time per class",
                     displayName = "K-th Percentile Local Write Transactions per class")
   public double getPercentileLocalRWriteTransactionParam(@Parameter(name = "Percentile") int percentile, @Parameter(name = "Transaction Class") String transactionalClass) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getPercentile(IspnStats.WR_LOCAL_PERCENTILE, percentile, transactionalClass));
   }

   @ManagedOperation(description = "K-th percentile of remote write transactions execution time per class",
                     displayName = "K-th Percentile Remote Write Transactions per class")
   public double getPercentileRemoteWriteTransactionParam(@Parameter(name = "Percentile") int percentile, @Parameter(name = "Transaction Class") String transactionalClass) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getPercentile(IspnStats.WR_REMOTE_PERCENTILE, percentile, transactionalClass));
   }

   @ManagedOperation(description = "Average Local processing Get time (in microseconds) per class",
                     displayName = "Average Local Get time per class")
   public long getAvgLocalGetTimeParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((IspnStats.LOCAL_GET_EXECUTION), transactionalClass)));
   }

   @ManagedOperation(description = "Average TCB time (in microseconds) per class",
                     displayName = "Average TCB time per class")
   public long getAvgTCBTimeParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((IspnStats.TBC), transactionalClass)));
   }

   @ManagedOperation(description = "Average NTCB time (in microseconds) per class",
                     displayName = "Average NTCB time per class")
   public long getAvgNTCBTimeParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((IspnStats.NTBC), transactionalClass)));
   }

   private boolean isRemote(Object key) {
      return distributionManager != null && !distributionManager.getLocality(key).isLocal();
   }

   private void replace() {
      log.infof("CustomStatsInterceptor Enabled!");
      ComponentRegistry componentRegistry = cache.getAdvancedCache().getComponentRegistry();

      GlobalComponentRegistry globalComponentRegistry = componentRegistry.getGlobalComponentRegistry();
      InboundInvocationHandlerWrapper invocationHandlerWrapper = rewireInvocationHandler(globalComponentRegistry);
      globalComponentRegistry.rewire();

      replaceFieldInTransport(componentRegistry, invocationHandlerWrapper);

      replaceRpcManager(componentRegistry);
      replaceLockManager(componentRegistry);
      replaceEntryFactoryWrapper(componentRegistry);
      componentRegistry.rewire();
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

   private void replaceEntryFactoryWrapper(ComponentRegistry componentRegistry) {
      EntryFactory entryFactory = componentRegistry.getComponent(EntryFactory.class);
      EntryFactoryWrapper entryFactoryWrapper = new EntryFactoryWrapper(entryFactory);
      componentRegistry.registerComponent(entryFactoryWrapper, EntryFactory.class);
   }

   private void replaceLockManager(ComponentRegistry componentRegistry) {
      LockManager lockManager = componentRegistry.getComponent(LockManager.class);
      LockManagerWrapper lockManagerWrapper = new LockManagerWrapper(lockManager, StreamLibContainer.getOrCreateStreamLibContainer(cache));
      componentRegistry.registerComponent(lockManagerWrapper, LockManager.class);
   }

   private void replaceRpcManager(ComponentRegistry componentRegistry) {
      RpcManager rpcManager = componentRegistry.getComponent(RpcManager.class);
      RpcManagerWrapper rpcManagerWrapper = new RpcManagerWrapper(rpcManager);
      componentRegistry.registerComponent(rpcManagerWrapper, RpcManager.class);
      this.rpcManager = rpcManagerWrapper;
   }

   private void initStatsIfNecessary(InvocationContext ctx) {
      if (ctx.isInTxScope())
         TransactionsStatisticsRegistry.initTransactionIfNecessary((TxInvocationContext) ctx);
   }

   private boolean isLockTimeout(TimeoutException e) {
      return e.getMessage().startsWith("Unable to acquire lock after");
   }

   private void updateTime(IspnStats duration, IspnStats counter, long initTime) {
      TransactionsStatisticsRegistry.addValue(duration, System.nanoTime() - initTime);
      TransactionsStatisticsRegistry.incrementValue(counter);
   }

   private long handleLong(Long value) {
      return value == null ? 0 : value;
   }

   private double handleDouble(Double value) {
      return value == null ? 0 : value;
   }
}
