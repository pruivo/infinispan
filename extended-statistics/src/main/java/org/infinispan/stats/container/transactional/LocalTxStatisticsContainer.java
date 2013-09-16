package org.infinispan.stats.container.transactional;

import org.infinispan.stats.container.DataAccessStatisticsContainer;
import org.infinispan.stats.container.EnumStatisticsContainer;
import org.infinispan.stats.container.NetworkStatisticsContainer;
import org.infinispan.util.TimeService;

/**
 * //TODO: document this!
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
public class LocalTxStatisticsContainer extends BaseTxStatisticsContainer implements DataAccessStatisticsContainer,
                                                                                     NetworkStatisticsContainer {

   private final EnumStatisticsContainer<DataAccessStatistics> dataAccessStatistics;
   private final EnumStatisticsContainer<NetworkStatistics> networkStatistics;
   private boolean readOnlyTransaction;

   public LocalTxStatisticsContainer(TimeService timeService) {
      super(timeService, timeService.time());
      this.dataAccessStatistics = new EnumStatisticsContainer<DataAccessStatistics>(DataAccessStatistics.class);
      this.networkStatistics = new EnumStatisticsContainer<NetworkStatistics>(NetworkStatistics.class);
      this.readOnlyTransaction = true;
   }

   @Override
   public void writeAccess(long duration) {
      addWriteAccess(DataAccessStatistics.NUM_WRITE, DataAccessStatistics.WRITE_DUR, duration);
   }

   @Override
   public void writeAccessLockTimeout(long duration) {
      addWriteAccess(DataAccessStatistics.NUM_WRITE_FAIL_LOCK_TIMEOUT, DataAccessStatistics.WRITE_DUR_FAIL_LOCK_TIMEOUT,
                     duration);
   }

   @Override
   public void writeAccessNetworkTimeout(long duration) {
      addWriteAccess(DataAccessStatistics.NUM_WRITE_FAIL_NETWORK_TIMEOUT,
                     DataAccessStatistics.WRITE_DUR_FAIL_NETWORK_TIMEOUT, duration);
   }

   @Override
   public void writeAccessDeadlock(long duration) {
      addWriteAccess(DataAccessStatistics.NUM_WRITE_FAIL_DEADLOCK, DataAccessStatistics.WRITE_DUR_FAIL_DEADLOCK,
                     duration);
   }

   @Override
   public void writeAccessValidationError(long duration) {
      addWriteAccess(DataAccessStatistics.NUM_WRITE_FAIL_VALIDATION, DataAccessStatistics.WRITE_DUR_FAIL_VALIDATION,
                     duration);
   }

   @Override
   public void writeAccessUnknownError(long duration) {
      addWriteAccess(DataAccessStatistics.NUM_WRITE_FAIL_UNKNOWN, DataAccessStatistics.WRITE_DUR_FAIL_UNKNOWN, duration);
   }

   @Override
   public void readAccess(long duration) {
      addAccess(DataAccessStatistics.NUM_READ, DataAccessStatistics.READ_DUR, duration);
   }

   @Override
   public void readAccessLockTimeout(long duration) {
      addAccess(DataAccessStatistics.NUM_READ_FAIL_LOCK_TIMEOUT, DataAccessStatistics.READ_DUR_FAIL_LOCK_TIMEOUT,
                duration);
   }

   @Override
   public void readAccessNetworkTimeout(long duration) {
      addAccess(DataAccessStatistics.NUM_READ_FAIL_NETWORK_TIMEOUT, DataAccessStatistics.READ_DUR_FAIL_NETWORK_TIMEOUT,
                duration);
   }

   @Override
   public void readAccessDeadlock(long duration) {
      addAccess(DataAccessStatistics.NUM_READ_FAIL_DEADLOCK, DataAccessStatistics.READ_DUR_FAIL_DEADLOCK, duration);
   }

   @Override
   public void readAccessValidationError(long duration) {
      addAccess(DataAccessStatistics.NUM_READ_FAIL_VALIDATION, DataAccessStatistics.READ_DUR_FAIL_VALIDATION, duration);
   }

   @Override
   public void readAccessUnknownError(long duration) {
      addAccess(DataAccessStatistics.NUM_READ_FAIL_UNKNOWN, DataAccessStatistics.READ_DUR_FAIL_UNKNOWN, duration);
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

   private void addWriteAccess(DataAccessStatistics counterStat, DataAccessStatistics durationStat, long duration) {
      this.readOnlyTransaction = false;
      addAccess(counterStat, durationStat, duration);
   }

   private void addAccess(DataAccessStatistics counterStat, DataAccessStatistics durationStat, long duration) {
      dataAccessStatistics.add(durationStat, duration);
      dataAccessStatistics.increment(counterStat);
   }

   private void addNetwork(NetworkStatistics rttStat, NetworkStatistics sizeStat, NetworkStatistics counterStat,
                           long rtt, long size) {
      networkStatistics.add(rttStat, rtt);
      networkStatistics.add(sizeStat, size);
      networkStatistics.increment(counterStat);
   }

}
