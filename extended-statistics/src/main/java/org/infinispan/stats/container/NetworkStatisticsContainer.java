package org.infinispan.stats.container;

/**
 * //TODO: document this!
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
public interface NetworkStatisticsContainer {

   void remoteGet(long rtt, int size);

   void prepare(long rtt, int size);

   void commit(long rtt, int size);

   void rollback(long rtt, int size);

}
