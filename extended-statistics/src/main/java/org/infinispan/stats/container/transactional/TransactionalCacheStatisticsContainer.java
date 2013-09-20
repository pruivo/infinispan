package org.infinispan.stats.container.transactional;

import org.infinispan.util.TimeService;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * //TODO: document this!
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
public class TransactionalCacheStatisticsContainer {

   private static final StatisticsSnapshot EMPTY = new StatisticsSnapshot() {
      @Override
      public long getLastResetTimeStamp() {
         return 0;
      }

      @Override
      public float getStats(TxOutcome outcome, TxExtendedStatistic statistic) {
         return 0;
      }
   };
   private final TimeService timeService;
   private final AtomicBoolean reseting;
   private volatile long lastResetTimestamp;
   private volatile long[] successStats;
   private volatile long[] lockTimeoutStats;
   private volatile long[] deadlockStats;
   private volatile long[] networkTimeoutStats;
   private volatile long[] validationStats;
   private volatile long[] unknownErrorStats;

   public TransactionalCacheStatisticsContainer(TimeService timeService) {
      this.timeService = timeService;
      reseting = new AtomicBoolean(false);
      reset();
   }

   public final StatisticsSnapshot getSnapshot() {
      if (reseting.get()) {
         return EMPTY;
      }
      return new StatisticSnapshotImpl(lastResetTimestamp, successStats, lockTimeoutStats, deadlockStats,
                                       networkTimeoutStats, validationStats, unknownErrorStats);
   }

   public final void reset() {
      if (!reseting.compareAndSet(false, true)) {
         return; //reset in progress
      }
      successStats = null;
      lockTimeoutStats = null;
      deadlockStats = null;
      networkTimeoutStats = null;
      validationStats = null;
      unknownErrorStats = null;
      lastResetTimestamp = timeService.time();
      reseting.set(false);
   }

   public final void update(TxOutcome outcome, Map<TxExtendedStatistic, Long> stats) {
      if (stats == null || stats.isEmpty() || reseting.get()) {
         return;
      }
      switch (outcome) {
         case SUCCESS:
            successStats = addStats(stats, successStats);
            break;
         case LOCK_TIMEOUT:
            lockTimeoutStats = addStats(stats, lockTimeoutStats);
            break;
         case DEADLOCK:
            deadlockStats = addStats(stats, deadlockStats);
            break;
         case NETWORK_TIMEOUT:
            networkTimeoutStats = addStats(stats, networkTimeoutStats);
            break;
         case VALIDATION:
            validationStats = addStats(stats, validationStats);
            break;
         case UNKNOWN_ERROR:
            unknownErrorStats = addStats(stats, unknownErrorStats);
            break;
      }
   }

   private long[] addStats(Map<TxExtendedStatistic, Long> stats, long[] existing) {
      final long[] array = new long[TxExtendedStatistic.values().length];
      if (existing != null) {
         System.arraycopy(existing, 0, array, 0, array.length);
      } else {
         Arrays.fill(array, 0);
      }
      for (Map.Entry<TxExtendedStatistic, Long> entry : stats.entrySet()) {
         final int index = entry.getKey().ordinal();
         array[index] = array[index] + entry.getValue();
      }
      return array;
   }

   private class StatisticSnapshotImpl implements StatisticsSnapshot {
      private final long lastResetTimestamp;
      private final long[] successStats;
      private final long[] lockTimeoutStats;
      private final long[] deadlockStats;
      private final long[] networkTimeoutStats;
      private final long[] validationStats;
      private final long[] unknownErrorStats;

      private StatisticSnapshotImpl(long lastResetTimestamp, long[] successStats, long[] lockTimeoutStats,
                                    long[] deadlockStats, long[] networkTimeoutStats, long[] validationStats,
                                    long[] unknownErrorStats) {
         this.lastResetTimestamp = lastResetTimestamp;
         this.successStats = successStats;
         this.lockTimeoutStats = lockTimeoutStats;
         this.deadlockStats = deadlockStats;
         this.networkTimeoutStats = networkTimeoutStats;
         this.validationStats = validationStats;
         this.unknownErrorStats = unknownErrorStats;
      }

      @Override
      public long getLastResetTimeStamp() {
         return lastResetTimestamp;
      }

      @Override
      public float getStats(TxOutcome outcome, TxExtendedStatistic statistic) {
         if (outcome == null || statistic == null) {
            return 0;
         }
         final long[] stats = getStatsFor(outcome);
         return stats == null ? 0 : stats[statistic.ordinal()];
      }

      private long[] getStatsFor(TxOutcome outcome) {
         switch (outcome) {
            case SUCCESS:
               return successStats;
            case LOCK_TIMEOUT:
               return lockTimeoutStats;
            case DEADLOCK:
               return deadlockStats;
            case NETWORK_TIMEOUT:
               return networkTimeoutStats;
            case VALIDATION:
               return validationStats;
            case UNKNOWN_ERROR:
               return unknownErrorStats;
            default:
               return null;
         }
      }
   }

}
