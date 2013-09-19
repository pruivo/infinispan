package org.infinispan.stats.manager;

import org.infinispan.context.InvocationContext;
import org.infinispan.stats.container.DataAccessStatisticsContainer;
import org.infinispan.stats.container.LockStatisticsContainer;
import org.infinispan.stats.container.NetworkStatisticsContainer;
import org.infinispan.stats.container.transactional.TransactionStatisticsContainer;

/**
 * //TODO: document this!
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
public interface StatisticsManager {

   DataAccessStatisticsContainer getDataAccessStatisticsContainer(InvocationContext context);

   LockStatisticsContainer getLockStatisticsContainer(Object owner);

   NetworkStatisticsContainer getNetworkStatisticsContainer(InvocationContext context);

   TransactionStatisticsContainer getTransactionStatisticsContainer(InvocationContext context);

}
