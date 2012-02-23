package org.infinispan.transaction.totalorder;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.interceptors.locking.OptimisticLockingInterceptor;
import org.infinispan.jmx.annotations.DisplayType;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.Parameter;
import org.infinispan.jmx.annotations.Units;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.transaction.RemoteTransaction;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author mircea.markus@jboss.com
 * @author Pedro Ruivo
 * @since 5.2.0
 */
public abstract class BaseTotalOrderManager implements TotalOrderManager {

   protected final Log log = LogFactory.getLog(getClass());
   protected final AtomicLong processingDuration = new AtomicLong(0);
   protected final AtomicInteger numberOfTxValidated = new AtomicInteger(0);
   protected Configuration configuration;
   protected TransactionTable transactionTable;
   protected CommandsFactory commandsFactory;
   protected RpcManager rpcManager;
   protected boolean needsAcks;
   protected boolean trace;
   protected volatile boolean statisticsEnabled;
   private StateTransferManager stateTransferManager;
   private ClusteringDependentLogic clusteringDependentLogic;

   @Inject
   public void inject(Configuration configuration, InvocationContextContainer invocationContextContainer,
                      TransactionTable transactionTable, CommandsFactory commandsFactory,
                      RpcManager rpcManager, StateTransferManager stateTransferManager, ClusteringDependentLogic clusteringDependentLogic) {
      this.configuration = configuration;
      this.transactionTable = transactionTable;
      this.commandsFactory = commandsFactory;
      this.rpcManager = rpcManager;
      this.stateTransferManager = stateTransferManager;
      this.clusteringDependentLogic = clusteringDependentLogic;
   }

   @Start
   public void start() {
      setStatisticsEnabled(configuration.jmxStatistics().enabled());
      needsAcks = configuration.clustering().cacheMode().isSynchronous() &&
            configuration.transaction().syncCommitPhase();
      trace = log.isTraceEnabled();
   }

   @Override
   public final void finishTransaction(RemoteTransaction remoteTransaction, boolean commit) {
      GlobalTransaction globalTransaction = remoteTransaction.getGlobalTransaction();
      if (trace) log.tracef("transaction %s is finished", globalTransaction.prettyPrint());

      releaseResources(remoteTransaction);
      transactionCompleted(globalTransaction, commit);
   }

   @ManagedAttribute(description = "Average duration of a transaction validation (milliseconds)",
                     displayName = "Average Validation Duration", units = Units.MILLISECONDS, displayType = DisplayType.SUMMARY)
   public double getAverageValidationDuration() {
      long time = processingDuration.get();
      int tx = numberOfTxValidated.get();
      if (tx == 0) {
         return 0;
      }
      return (time / tx) / 1000000.0;
   }

   @ManagedOperation(description = "Resets the statistics", displayName = "Reset Statistic")
   public void resetStatistics() {
      processingDuration.set(0);
      numberOfTxValidated.set(0);
   }

   @ManagedAttribute(description = "Show it the gathering of statistics is enabled", displayName = "Statistic Enabled?")
   public boolean isStatisticsEnabled() {
      return statisticsEnabled;
   }

   @ManagedOperation(description = "Enables or disables the gathering of statistics by this component", displayName = "Enable Statistic")
   public void setStatisticsEnabled(@Parameter(name = "enabled", description = "true to enable statistic, false otherwise")
                                       boolean statisticsEnabled) {
      this.statisticsEnabled = statisticsEnabled;
   }

   protected final void awaitIncomingStateTransfer(Collection<WriteCommand> modifications) throws InterruptedException {
      Object[] keys = getKeysToLock(modifications);
      Collection<CountDownLatch> latchCollection = stateTransferManager.getInboundStateTransferLatches(keys);

      if (log.isDebugEnabled()) {
         log.debugf("[%s] Transaction touches in %s keys and needs to wait from %s latches for the incoming state",
                    Thread.currentThread().getName(), keys == null ? "all" : keys.length, latchCollection.size());
      }

      for (CountDownLatch latch : latchCollection) {
         latch.await();
      }

      if (log.isDebugEnabled()) {
         log.debugf("[%s] Transaction has finished to wait for the incoming state", Thread.currentThread().getName());
      }
   }

   protected final boolean isIncomingStateTransfer(Collection<WriteCommand> modifications) {
      Object[] keys = getKeysToLock(modifications);
      Collection<CountDownLatch> latchCollection = stateTransferManager.getInboundStateTransferLatches(keys);

      return !isAllZero(latchCollection);
   }

   protected final boolean isAllZero(Collection<? extends CountDownLatch> collection) {
      for (CountDownLatch countDownLatch : collection) {
         try {
            if (!countDownLatch.await(0, TimeUnit.MILLISECONDS)) {
               return false;
            }
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
         }
      }
      return true;
   }

   /**
    * copy the looked up entries from local context to the remote context
    *
    * @param ctx the remote context
    */
   protected final void copyLookedUpEntriesToRemoteContext(TxInvocationContext ctx) {
      //TODO: fix me
      /*LocalTransaction localTransaction = transactionTable.getLocalTransaction(ctx.getGlobalTransaction());
      if (localTransaction != null) {
         ctx.putLookedUpEntries(localTransaction.getLookedUpEntries());
      }*/
   }

   /**
    * it logs a new transaction delivered by the sequencer and checks if the context is remote
    *
    * @param prepareCommand the prepare command
    * @param ctx            the remote context
    */
   protected final void logAndCheckContext(PrepareCommand prepareCommand, TxInvocationContext ctx) {
      if (trace)
         log.tracef("Processing transaction from sequencer: %s", prepareCommand.getGlobalTransaction().prettyPrint());

      if (ctx.isOriginLocal()) throw new IllegalArgumentException("Local invocation not allowed!");
   }

   /**
    * calculates the keys affected by the list of modification. This method should return only the key own by this node
    *
    * @param modifications the list of modifications
    * @return a set of local keys (or null if the modification list has a clear command)
    */
   protected Object[] getKeysToLock(Collection<WriteCommand> modifications) {
      WriteCommand[] writeCommands = new WriteCommand[modifications.size()];
      writeCommands = modifications.toArray(writeCommands);
      return filterKey(OptimisticLockingInterceptor.sort(writeCommands, clusteringDependentLogic));
   }

   /**
    * mark the remote transaction as completed, i.e., finished and committed or rollbacked
    *
    * @param globalTransaction the global transaction
    * @param commit            true if the transaction was commited, false otherwise
    */
   protected final void transactionCompleted(GlobalTransaction globalTransaction, boolean commit) {
      try {
         if (commit) {
            transactionTable.remoteTransactionCommitted(globalTransaction);
         } else {
            transactionTable.remoteTransactionRollback(globalTransaction);
         }
      } catch (Exception e) {
         log.warnf("Exception suppressed while mark transaction %s as completed (%s)", globalTransaction.prettyPrint(),
                   commit ? "commit" : "rollback");
      }
   }

   // =========== JMX ==========

   /**
    * returns the current time in nanoseconds
    *
    * @return the current time in nanoseconds (only if the statistics are enabled)
    */
   protected final long now() {
      //we know that this is only used for stats
      return statisticsEnabled ? System.nanoTime() : -1;
   }

   /**
    * release the resources allocated by the remote transaction. Used when the transaction is completed or when the
    * transaction is marked for rollback
    *
    * @param remoteTransaction the remote transaction
    */
   protected abstract void releaseResources(RemoteTransaction remoteTransaction);

   private Object[] filterKey(Object[] keys) {
      if (keys == null) {
         return null;
      } else if (keys.length == 0) {
         return keys;
      }
      Set<Object> localKeys = new HashSet<Object>();
      for (Object key : keys) {
         if (clusteringDependentLogic.localNodeIsOwner(key)) {
            localKeys.add(key);
         }
      }
      return localKeys.toArray();
   }

   private void logProcessingFinalStatus(GlobalTransaction globalTransaction, Object result) {
      if (trace) {
         boolean exception = result instanceof Throwable;
         log.tracef("Transaction %s finished processing (%s). Validation result is %s ",
                    globalTransaction.prettyPrint(), (exception ? "failed" : "ok"),
                    (exception ? ((Throwable) result).getMessage() : result));
      }
   }
}
