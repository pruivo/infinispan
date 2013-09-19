package org.infinispan.stats.manager;

import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.stats.container.DataAccessStatisticsContainer;
import org.infinispan.stats.container.LockStatisticsContainer;
import org.infinispan.stats.container.NetworkStatisticsContainer;
import org.infinispan.stats.container.transactional.LocalTxStatisticsContainer;
import org.infinispan.stats.container.transactional.RemoteTxStatisticContainer;
import org.infinispan.stats.container.transactional.TransactionStatisticsContainer;
import org.infinispan.stats.container.transactional.TransactionalCacheStatisticsContainer;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.TimeService;

import java.util.concurrent.ConcurrentMap;

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

   public TransactionalStatisticsManager(TimeService timeService) {
      container = new TransactionalCacheStatisticsContainer(timeService);
      this.timeService = timeService;
   }

   @Override
   public DataAccessStatisticsContainer getDataAccessStatisticsContainer(InvocationContext context) {
      if (context.isOriginLocal()) {
         GlobalTransaction globalTransaction = extractGlobalTransaction(context);
         return globalTransaction == null ? null : getLocalContainer(globalTransaction);
      }
      return null;
   }

   @Override
   public LockStatisticsContainer getLockStatisticsContainer(Object owner) {
      if (owner instanceof GlobalTransaction) {
         GlobalTransaction globalTransaction = (GlobalTransaction) owner;
         return globalTransaction.isRemote() ? getRemoteContainer(globalTransaction) :
               getLocalContainer(globalTransaction);
      }
      return null;
   }

   @Override
   public NetworkStatisticsContainer getNetworkStatisticsContainer(InvocationContext context) {
      return null;  //To change body of implemented methods use File | Settings | File Templates.
   }

   @Override
   public TransactionStatisticsContainer getTransactionStatisticsContainer(InvocationContext context) {
      GlobalTransaction globalTransaction = extractGlobalTransaction(context);
      if (globalTransaction == null) {
         throw new IllegalStateException("Transaction expected!");
      }
      return context.isOriginLocal() ? getLocalContainer(globalTransaction) : getRemoteContainer(globalTransaction);
   }

   private GlobalTransaction extractGlobalTransaction(InvocationContext context) {
      if (context.isInTxScope()) {
         return ((TxInvocationContext) context).getGlobalTransaction();
      }
      return null;
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
}
