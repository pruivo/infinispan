package org.infinispan.stats.container.transactional;

/**
 * //TODO: document this!
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
public interface StatisticsSnapshot {

   public long getLastResetTimeStamp();

   public float getStats(TxOutcome outcome, TxExtendedStatistic statistic);

}
