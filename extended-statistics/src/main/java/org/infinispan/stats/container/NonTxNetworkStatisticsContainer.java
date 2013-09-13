package org.infinispan.stats.container;

/**
 * //TODO: document this!
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
public class NonTxNetworkStatisticsContainer implements NetworkStatisticsContainer {
   @Override
   public void remoteGet(long rtt, int size) {
      //To change body of implemented methods use File | Settings | File Templates.
   }

   @Override
   public void prepare(long rtt, int size) {
      //To change body of implemented methods use File | Settings | File Templates.
   }

   @Override
   public void commit(long rtt, int size) {
      //To change body of implemented methods use File | Settings | File Templates.
   }

   @Override
   public void rollback(long rtt, int size) {
      //To change body of implemented methods use File | Settings | File Templates.
   }
}
