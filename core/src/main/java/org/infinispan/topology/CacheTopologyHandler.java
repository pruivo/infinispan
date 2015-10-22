package org.infinispan.topology;

import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.statetransfer.StateTransferManager;

/**
 * The link between {@link LocalTopologyManager} and {@link StateTransferManager}.
 *
 * @author Dan Berindei
 * @author Pedro Ruivo
 * @since 5.2
 */
public interface CacheTopologyHandler {


   /**
    * Updates the cache topology immediately.
    * <p>
    * Used when a node leaves the cluster. Also, it receives the current coordinator state (in case if the update
    * happens at the same time as rebalance/other cache topology updates). The {@code newCH} argument is non-null when
    * the {@code state} is {@link TopologyState#REBALANCE}. The {@code newCH} is only used by the {@link
    * org.infinispan.notifications.cachelistener.CacheNotifier}.
    *
    * @param cacheTopology the new {@link CacheTopology}.
    * @param newCH         The new {@link ConsistentHash}. It may be {@code null}.
    * @param state         The coordinator {@link TopologyState}.
    */
   void updateConsistentHash(CacheTopology cacheTopology, ConsistentHash newCH, TopologyState state);

   /**
    * Invoked when state transfer has to be started.
    * <p>
    * The caller will not consider the local rebalance done when this method returns. Instead, the handler will have to
    * call {@link LocalTopologyManager#confirmRebalance(String, int, int, Throwable)}. The local node updates its state
    * to {@link TopologyState#REBALANCE}.
    * <p>
    * The {@code newCH} may be {@code null} and it is only used by the {@link org.infinispan.notifications.cachelistener.CacheNotifier}.
    *
    * @param cacheTopology The new {@link CacheTopology}.
    * @param newCH         The new {@link ConsistentHash}. It may be {@code null}.
    */
   void rebalance(CacheTopology cacheTopology, ConsistentHash newCH);

   /**
    * It updates only the read {@link ConsistentHash}.
    * <p>
    * After the state transfer, the read {@link ConsistentHash} is updated to involve only the old owners. The local
    * node updates its state to {@link TopologyState#READ_CH_UPDATE}.
    *
    * @param cacheTopology The new {@link CacheTopology}.
    */
   void updateReadConsistentHash(CacheTopology cacheTopology);

   /**
    * It updates the write {@link ConsistentHash}.
    * <p>
    * Finally, the write {@link ConsistentHash} is updated and the topology is stable. The lock node updates its state
    * to {@link TopologyState#NONE}.
    *
    * @param cacheTopology The new {@link CacheTopology}.
    */
   void updateWriteConsistentHash(CacheTopology cacheTopology);
}
