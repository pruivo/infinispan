package org.infinispan.stats.container.transactional;

import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.stats.container.DataAccessStatisticsContainer;
import org.infinispan.stats.container.EnumStatisticsContainer;
import org.infinispan.stats.container.NetworkStatisticsContainer;
import org.infinispan.util.TimeService;

import java.util.EnumMap;
import java.util.Map;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.infinispan.stats.container.transactional.TxExtendedStatistic.*;
import static org.infinispan.stats.container.transactional.TxOutcome.*;

/**
 * //TODO: document this!
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
public class LocalTxStatisticsContainer extends BaseTxStatisticsContainer implements DataAccessStatisticsContainer,
                                                                                     NetworkStatisticsContainer {
   private final DataAccessStats dataAccessStats;
   private final EnumStatisticsContainer<NetworkStatistics> networkStatistics;

   public LocalTxStatisticsContainer(TimeService timeService) {
      super(timeService, timeService.time());
      this.dataAccessStats = new DataAccessStats();
      this.networkStatistics = new EnumStatisticsContainer<NetworkStatistics>(NetworkStatistics.class);
   }

   @Override
   public void writeAccess(long duration) {
      addAccess(true, duration, SUCCESS);
   }

   @Override
   public void writeAccessLockTimeout(long duration) {
      addAccess(true, duration, LOCK_TIMEOUT);
   }

   @Override
   public void writeAccessNetworkTimeout(long duration) {
      addAccess(true, duration, NETWORK_TIMEOUT);
   }

   @Override
   public void writeAccessDeadlock(long duration) {
      addAccess(true, duration, DEADLOCK);
   }

   @Override
   public void writeAccessValidationError(long duration) {
      addAccess(true, duration, VALIDATION);
   }

   @Override
   public void writeAccessUnknownError(long duration) {
      addAccess(true, duration, UNKNOWN_ERROR);
   }

   @Override
   public void readAccess(long duration) {
      addAccess(false, duration, SUCCESS);
   }

   @Override
   public void readAccessLockTimeout(long duration) {
      addAccess(false, duration, LOCK_TIMEOUT);
   }

   @Override
   public void readAccessNetworkTimeout(long duration) {
      addAccess(false, duration, NETWORK_TIMEOUT);
   }

   @Override
   public void readAccessDeadlock(long duration) {
      addAccess(false, duration, DEADLOCK);
   }

   @Override
   public void readAccessValidationError(long duration) {
      addAccess(false, duration, VALIDATION);
   }

   @Override
   public void readAccessUnknownError(long duration) {
      addAccess(false, duration, UNKNOWN_ERROR);
   }

   @Override
   public void remoteGet(long rtt, int size) {
      addNetwork(NetworkStatistics.GET_RTT, NetworkStatistics.GET_SIZE, NetworkStatistics.NUM_GET, rtt, size);
   }

   @Override
   public void prepare(long rtt, int size) {
      addNetwork(NetworkStatistics.PREPARE_RTT, NetworkStatistics.PREPARE_SIZE, NetworkStatistics.NUM_PREPARE, rtt, size);
   }

   @Override
   public void commit(long rtt, int size) {
      addNetwork(NetworkStatistics.COMMIT_RTT, NetworkStatistics.COMMIT_SIZE, NetworkStatistics.NUM_COMMIT, rtt, size);
   }

   @Override
   public void rollback(long rtt, int size) {
      addNetwork(NetworkStatistics.ROLLBACK_RTT, NetworkStatistics.ROLLBACK_SIZE, NetworkStatistics.NUM_ROLLBACK, rtt,
                 size);
   }

   public final Map<TxExtendedStatistic, Long> merge() {
      if (dataAccessStats.outcome == null || txStats.outcome == null) {
         return InfinispanCollections.emptyMap();
      }

      Map<TxExtendedStatistic, Long> map = createMap();
      final boolean readOnly = dataAccessStats.numWrites == 0;

      mergeAccesses(map, readOnly);
      mergeLocks(map, readOnly);
      mergeNetwork(map, readOnly);
      mergeCommandCounters(map);
      mergeTransactionDuration(map, readOnly);
      return map;
   }

   public final TxOutcome getTxOutcome() {
      if (dataAccessStats.outcome == SUCCESS && txStats.outcome == SUCCESS) {
         return SUCCESS;
      } else if (dataAccessStats.outcome != SUCCESS) {
         return dataAccessStats.outcome;
      } else {
         return txStats.outcome;
      }
   }

   private void mergeTransactionDuration(Map<TxExtendedStatistic, Long> map, boolean readOnly) {
      final long execution = timeService.timeDuration(txStats.startRunningTimeStamp,
                                                      txStats.endRunningTimeStamp, NANOSECONDS);
      final long termination = timeService.timeDuration(txStats.endRunningTimeStamp,
                                                        txStats.endCommittingTimeStamp, NANOSECONDS);
      if (readOnly) {
         map.put(NUM_LOCAL_RD_TX, 1L);
         map.put(EXEC_DUR_LOCAL_RD_TX, execution);
         map.put(COMMIT_DUR_LOCAL_RD_TX, termination);
      } else {
         map.put(NUM_LOCAL_WRT_TX, 1L);
         map.put(EXEC_DUR_LOCAL_WRT_TX, execution);
         map.put(COMMIT_DUR_LOCAL_WRT_TX, termination);
      }
   }

   private void mergeAccesses(Map<TxExtendedStatistic, Long> map, boolean readOnly) {
      if (readOnly) {
         map.put(NUM_READ_RD_TX, (long) dataAccessStats.numReads);
         map.put(READ_DUR_RD_TX, dataAccessStats.readDuration);
      } else {
         map.put(NUM_READ_WRT_TX, (long) dataAccessStats.numReads);
         map.put(READ_DUR_WRT_TX, dataAccessStats.readDuration);
         map.put(NUM_WRITE_WRT_TX, (long) dataAccessStats.numWrites);
         map.put(WRITE_DUR_WRT_TX, dataAccessStats.writeDuration);
      }
   }

   private void mergeLocks(Map<TxExtendedStatistic, Long> map, boolean readOnly) {
      if (readOnly) {
         map.put(NUM_LOCK_LOCAL_RD_TX, (long) lockStats.locksAcquired);
         map.put(LOCK_HOLD_TIME_LOCAL_RD_TX, lockStats.holdTime);
         map.put(NUM_LOCK_WAITING_LOCAL_RD_TX, (long) lockStats.lockWaited);
         map.put(LOCK_HOLDING_TIME_LOCAL_RD_TX, lockStats.waitingTime);
      } else {
         map.put(NUM_LOCK_LOCAL_WRT_TX, (long) lockStats.locksAcquired);
         map.put(LOCK_HOLD_TIME_LOCAL_WRT_TX, lockStats.holdTime);
         map.put(NUM_LOCK_WAITING_LOCAL_WRT_TX, (long) lockStats.lockWaited);
         map.put(LOCK_HOLDING_TIME_LOCAL_WRT_TX, lockStats.waitingTime);
      }
   }

   private void mergeNetwork(Map<TxExtendedStatistic, Long> map, boolean readOnly) {
      if (readOnly) {
         map.put(NUM_REMOTE_GET_RD_TX, networkStatistics.get(NetworkStatistics.NUM_GET));
         map.put(REMOTE_GET_RTT_RD_TX, networkStatistics.get(NetworkStatistics.GET_RTT));
         map.put(REMOTE_GET_SIZE_RD_TX, networkStatistics.get(NetworkStatistics.GET_SIZE));
         map.put(NUM_ROLLBACK_SENT_RD_TX, networkStatistics.get(NetworkStatistics.NUM_ROLLBACK));
         map.put(ROLLBACK_SENT_RTT_RD_TX, networkStatistics.get(NetworkStatistics.ROLLBACK_RTT));
         map.put(ROLLBACK_SENT_SIZE_RD_TX, networkStatistics.get(NetworkStatistics.ROLLBACK_SIZE));
      } else {
         map.put(NUM_REMOTE_GET_WRT_TX, networkStatistics.get(NetworkStatistics.NUM_GET));
         map.put(REMOTE_GET_RTT_WRT_TX, networkStatistics.get(NetworkStatistics.GET_RTT));
         map.put(REMOTE_GET_SIZE_WRT_TX, networkStatistics.get(NetworkStatistics.GET_SIZE));
         map.put(NUM_PREPARE_SENT_WRT_TX, networkStatistics.get(NetworkStatistics.NUM_PREPARE));
         map.put(PREPARE_SENT_RTT_WRT_TX, networkStatistics.get(NetworkStatistics.PREPARE_RTT));
         map.put(PREPARE_SENT_SIZE_WRT_TX, networkStatistics.get(NetworkStatistics.PREPARE_SIZE));
         map.put(NUM_COMMIT_SENT_WRT_TX, networkStatistics.get(NetworkStatistics.NUM_COMMIT));
         map.put(COMMIT_SENT_RTT_WRT_TX, networkStatistics.get(NetworkStatistics.COMMIT_RTT));
         map.put(COMMIT_SENT_SIZE_WRT_TX, networkStatistics.get(NetworkStatistics.COMMIT_SIZE));
         map.put(NUM_ROLLBACK_SENT_WRT_TX, networkStatistics.get(NetworkStatistics.NUM_ROLLBACK));
         map.put(ROLLBACK_SENT_RTT_WRT_TX, networkStatistics.get(NetworkStatistics.ROLLBACK_RTT));
         map.put(ROLLBACK_SENT_SIZE_WRT_TX, networkStatistics.get(NetworkStatistics.ROLLBACK_SIZE));
      }
   }

   private void mergeCommandCounters(Map<TxExtendedStatistic, Long> map) {
      if (txStats.prepareCommandDuration > 0) {
         map.put(NUM_PREPARE_LOCAL, 1L);
         map.put(PREPARE_LOCAL_DUR, txStats.prepareCommandDuration);
      }
      if (txStats.commitCommandDuration > 0) {
         map.put(NUM_COMMIT_LOCAL, 1L);
         map.put(COMMIT_LOCAL_DUR, txStats.commitCommandDuration);
      }
      if (txStats.rollbackCommandDuration > 0) {
         map.put(NUM_ROLLBACK_LOCAL, 1L);
         map.put(ROLLBACK_LOCAL_DUR, txStats.rollbackCommandDuration);
      }
   }

   private Map<TxExtendedStatistic, Long> createMap() {
      return new EnumMap<TxExtendedStatistic, Long>(TxExtendedStatistic.class);
   }

   private void addAccess(boolean write, long duration, TxOutcome outcome) {
      dataAccessStats.updateOutcome(outcome);
      if (write && outcome == SUCCESS) {
         dataAccessStats.numWrites++;
         dataAccessStats.writeDuration += duration;
      } else if (outcome == SUCCESS) {
         dataAccessStats.numReads++;
         dataAccessStats.readDuration += duration;
      }
   }

   private void addNetwork(NetworkStatistics rttStat, NetworkStatistics sizeStat, NetworkStatistics counterStat,
                           long rtt, long size) {
      networkStatistics.add(rttStat, rtt);
      networkStatistics.add(sizeStat, size);
      networkStatistics.increment(counterStat);
   }

   private class DataAccessStats {
      private int numWrites;
      private long writeDuration;
      private int numReads;
      private long readDuration;
      private TxOutcome outcome;

      public void updateOutcome(TxOutcome outcome) {
         if (this.outcome == null) {
            this.outcome = outcome;
         } else if (this.outcome == SUCCESS) {
            this.outcome = outcome;
         } else if (this.outcome == UNKNOWN_ERROR && outcome != SUCCESS) {
            this.outcome = outcome;
         }
      }
   }

}
