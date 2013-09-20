package org.infinispan.stats.manager;

import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.Units;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.stats.container.DataAccessStatisticsContainer;
import org.infinispan.stats.container.LockStatisticsContainer;
import org.infinispan.stats.container.NetworkStatisticsContainer;
import org.infinispan.stats.container.transactional.LocalTxStatisticsContainer;
import org.infinispan.stats.container.transactional.RemoteTxStatisticContainer;
import org.infinispan.stats.container.transactional.StatisticsSnapshot;
import org.infinispan.stats.container.transactional.TransactionStatisticsContainer;
import org.infinispan.stats.container.transactional.TransactionalCacheStatisticsContainer;
import org.infinispan.stats.container.transactional.TxExtendedStatistic;
import org.infinispan.stats.container.transactional.TxOutcome;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.TimeService;

import java.util.concurrent.ConcurrentMap;

import static org.infinispan.stats.container.transactional.TxExtendedStatistic.*;
import static org.infinispan.stats.container.transactional.TxOutcome.*;

/**
 * //TODO: document this!
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
public class TransactionalStatisticsManager implements StatisticsManager {

   /**
    * collects statistic for a remote transaction
    */
   private final ConcurrentMap<GlobalTransaction, RemoteTxStatisticContainer> remoteTransactionStatistics =
         CollectionFactory.makeConcurrentMap();
   /**
    * collects statistic for a local transaction
    */
   private final ConcurrentMap<GlobalTransaction, LocalTxStatisticsContainer> localTransactionStatistics =
         CollectionFactory.makeConcurrentMap();
   private final TimeService timeService;
   private final TransactionalCacheStatisticsContainer container;
   private final RpcManager rpcManager;

   public TransactionalStatisticsManager(TimeService timeService, RpcManager rpcManager) {
      this.rpcManager = rpcManager;
      container = new TransactionalCacheStatisticsContainer(timeService);
      this.timeService = timeService;
   }

   @Override
   public DataAccessStatisticsContainer getDataAccessStatisticsContainer(GlobalTransaction globalTransaction,
                                                                         boolean local) {
      if (globalTransaction == null) {
         return null;
      }
      final boolean localTx = isLocal(globalTransaction);
      if (localTx && local) {
         return getLocalContainer(globalTransaction);
      }
      return null;
   }

   @Override
   public LockStatisticsContainer getLockStatisticsContainer(Object owner) {
      if (owner instanceof GlobalTransaction) {
         GlobalTransaction globalTransaction = (GlobalTransaction) owner;
         return isLocal(globalTransaction) ? getLocalContainer(globalTransaction) :
               getRemoteContainer(globalTransaction);
      }
      return null;
   }

   @Override
   public NetworkStatisticsContainer getNetworkStatisticsContainer(GlobalTransaction globalTransaction) {
      return globalTransaction != null && isLocal(globalTransaction) ? getLocalContainer(globalTransaction) : null;
   }

   @Override
   public TransactionStatisticsContainer getTransactionStatisticsContainer(GlobalTransaction globalTransaction,
                                                                           boolean local) {
      if (globalTransaction == null) {
         throw new IllegalStateException("Transaction expected!");
      }
      final boolean localTx = isLocal(globalTransaction);
      if (localTx && local) {
         return getLocalContainer(globalTransaction);
      } else if (!localTx && !local) {
         return getRemoteContainer(globalTransaction);
      }
      return null;
   }

   @ManagedAttribute(description = "Returns the average number of write per transaction, for successful transactions",
                     displayName = "Avg no of writes per success transaction")
   public final float getAvgNumWritesForSuccessTx() {
      return average(SUCCESS, NUM_WRITE_WRT_TX, NUM_LOCAL_WRT_TX);
   }

   @ManagedAttribute(description = "Returns the average duration of a write operation in successful transactions",
                     displayName = "Avg write duration in successful transaction",
                     units = Units.MILLISECONDS)
   public final float getAvgWriteDurationForSuccessTx() {
      return nanosToMillis(average(SUCCESS, WRITE_DUR_WRT_TX, NUM_WRITE_WRT_TX));
   }

   @ManagedAttribute(description = "Returns the average number of write per transaction, for transactions that has " +
         "failed due to lock timeout",
                     displayName = "Avg no of writes per failed transaction - lock")
   public final float getAvgNumWritesForLockTimeoutFailedTx() {
      return average(LOCK_TIMEOUT, NUM_WRITE_WRT_TX, NUM_LOCAL_WRT_TX);
   }

   @ManagedAttribute(description = "Returns the average duration of a write operation in lock timeout failed" +
         " transactions",
                     displayName = "Avg write duration in failed transaction - lock",
                     units = Units.MILLISECONDS)
   public final float getAvgWriteDurationForLockTimeoutFailedTx() {
      return nanosToMillis(average(LOCK_TIMEOUT, WRITE_DUR_WRT_TX, NUM_WRITE_WRT_TX));
   }

   @ManagedAttribute(description = "Returns the average number of write per transaction, for transactions that has " +
         "failed due to network timeout",
                     displayName = "Avg no of writes per failed transaction - network")
   public final float getAvgNumWritesForNetworkTimeoutFailedTx() {
      return average(NETWORK_TIMEOUT, NUM_WRITE_WRT_TX, NUM_LOCAL_WRT_TX);
   }

   @ManagedAttribute(description = "Returns the average duration of a write operation in network timeout failed" +
         " transactions",
                     displayName = "Avg write duration in failed transaction - network",
                     units = Units.MILLISECONDS)
   public final float getAvgWriteDurationForNetworkTimeoutFailedTx() {
      return nanosToMillis(average(NETWORK_TIMEOUT, WRITE_DUR_WRT_TX, NUM_WRITE_WRT_TX));
   }

   @ManagedAttribute(description = "Returns the average number of write per transaction, for transactions that has " +
         "failed due to deadlocks",
                     displayName = "Avg no of writes per failed transaction - deadlock")
   public final float getAvgNumWritesForDeadlockFailedTx() {
      return average(DEADLOCK, NUM_WRITE_WRT_TX, NUM_LOCAL_WRT_TX);
   }

   @ManagedAttribute(description = "Returns the average duration of a write operation in deadlock failed" +
         " transactions",
                     displayName = "Avg write duration in failed transaction - deadlock",
                     units = Units.MILLISECONDS)
   public final float getAvgWriteDurationForDeadlockFailedTx() {
      return nanosToMillis(average(DEADLOCK, WRITE_DUR_WRT_TX, NUM_WRITE_WRT_TX));
   }

   @ManagedAttribute(description = "Returns the average number of write per transaction, for transactions that has " +
         "failed due to validation",
                     displayName = "Avg no of writes per failed transaction - validation")
   public final float getAvgNumWritesForValidationFailedTx() {
      return average(VALIDATION, NUM_WRITE_WRT_TX, NUM_LOCAL_WRT_TX);
   }

   @ManagedAttribute(description = "Returns the average duration of a write operation in validation failed" +
         " transactions",
                     displayName = "Avg write duration in failed transaction - validation",
                     units = Units.MILLISECONDS)
   public final float getAvgWriteDurationForValidationFailedTx() {
      return nanosToMillis(average(VALIDATION, WRITE_DUR_WRT_TX, NUM_WRITE_WRT_TX));
   }

   @ManagedAttribute(description = "Returns the average number of write per transaction, for transactions that has " +
         "failed due to an unknown error",
                     displayName = "Avg no of writes per failed transaction - unknown")
   public final float getAvgNumWritesForUnknownFailedTx() {
      return average(UNKNOWN_ERROR, NUM_WRITE_WRT_TX, NUM_LOCAL_WRT_TX);
   }

   @ManagedAttribute(description = "Returns the average duration of a write operation in unknown error failed" +
         " transactions",
                     displayName = "Avg write duration in failed transaction - unknown",
                     units = Units.MILLISECONDS)
   public final float getAvgWriteDurationForUnknownFailedTx() {
      return nanosToMillis(average(UNKNOWN_ERROR, WRITE_DUR_WRT_TX, NUM_WRITE_WRT_TX));
   }

   private boolean isLocal(GlobalTransaction globalTransaction) {
      return rpcManager.getAddress().equals(globalTransaction.getAddress());
   }

   private LocalTxStatisticsContainer getLocalContainer(GlobalTransaction globalTransaction) {
      LocalTxStatisticsContainer localContainer = new LocalTxStatisticsContainer(timeService);
      LocalTxStatisticsContainer existing = localTransactionStatistics.putIfAbsent(globalTransaction, localContainer);
      return existing != null ? existing : localContainer;
   }

   private RemoteTxStatisticContainer getRemoteContainer(GlobalTransaction globalTransaction) {
      RemoteTxStatisticContainer remoteContainer = new RemoteTxStatisticContainer(timeService);
      RemoteTxStatisticContainer existing = remoteTransactionStatistics.putIfAbsent(globalTransaction, remoteContainer);
      return existing != null ? existing : remoteContainer;
   }

   private float average(TxOutcome outcome, TxExtendedStatistic denStat, TxExtendedStatistic numStat) {
      return average(container.getSnapshot(), outcome, denStat, numStat);
   }

   private float average(StatisticsSnapshot snapshot, TxOutcome outcome, TxExtendedStatistic numStat,
                         TxExtendedStatistic denStat) {
      float den = snapshot.getStats(outcome, denStat);
      return den == 0 ? 0 : snapshot.getStats(outcome, numStat) / den;
   }

   private float nanosToMillis(float nanos) {
      return nanos / 1E6f;
   }
}
