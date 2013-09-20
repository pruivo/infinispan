package org.infinispan.stats.container;

/**
 * //TODO: document this!
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
public interface NetworkStatisticsContainer {

   void remoteGet(long rtt, int size, int nrInvolvedNodes);

   void prepare(long rtt, int size, int nrInvolvedNodes);

   void commit(long rtt, int size, int nrInvolvedNodes);

   void rollback(long rtt, int size, int nrInvolvedNodes);

}
