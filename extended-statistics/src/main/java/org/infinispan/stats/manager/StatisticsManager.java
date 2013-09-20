package org.infinispan.stats.manager;

import org.infinispan.stats.container.DataAccessStatisticsContainer;
import org.infinispan.stats.container.LockStatisticsContainer;
import org.infinispan.stats.container.NetworkStatisticsContainer;
import org.infinispan.stats.container.transactional.TransactionStatisticsContainer;
import org.infinispan.transaction.xa.GlobalTransaction;

/**
 * //TODO: document this!
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
public interface StatisticsManager {

   DataAccessStatisticsContainer getDataAccessStatisticsContainer(GlobalTransaction globalTransaction, boolean local);

   LockStatisticsContainer getLockStatisticsContainer(Object owner);

   NetworkStatisticsContainer getNetworkStatisticsContainer(GlobalTransaction globalTransaction);

   TransactionStatisticsContainer getTransactionStatisticsContainer(GlobalTransaction globalTransaction, boolean local);

}
