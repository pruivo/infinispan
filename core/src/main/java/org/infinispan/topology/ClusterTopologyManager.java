package org.infinispan.topology;

import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.remoting.transport.Address;

/**
 * Maintains the topology for all the caches in the cluster.
 *
 * @author Dan Berindei
 * @author Pedro Ruivo
 * @since 5.2
 */
@Scope(Scopes.GLOBAL)
public interface ClusterTopologyManager {
   /**
    * Signals that a new member is joining the cache.
    * <p>
    * The returned {@code CacheStatusResponse.cacheTopology} is the current cache topology before the node joined. If
    * the node is the first to join the cache, the returned topology does include the joiner, and it is never {@code
    * null}.
    */
   CacheStatusResponse handleJoin(String cacheName, Address joiner, CacheJoinInfo joinInfo, int viewId) throws Exception;

   /**
    * Signals that a member is leaving the cache.
    */
   void handleLeave(String cacheName, Address leaver, int viewId) throws Exception;

   /**
    * Marks the rebalance as complete on the sender.
    */
   void handleRebalanceCompleted(String cacheName, Address node, int topologyId, Throwable throwable, int viewId);

   /**
    * Marks the installation of read {@link ConsistentHash} completed on the sender.
    */
   void handleReadCHCompleted(String cacheName, Address node, int topologyId, Throwable throwable, int viewId);

   /**
    * Install a new cluster view.
    */
   void handleClusterView(boolean isMerge, int viewId);

   /**
    * Broadcasts the rebalance start request.
    */
   void broadcastRebalanceStart(String cacheName, CacheTopology cacheTopology, ConsistentHash newConsistentHash,
                                boolean totalOrder);

   /**
    * Broadcasts a read {@link ConsistentHash} update.
    */
   void broadcastReadConsistentHashUpdate(String cacheName, CacheTopology cacheTopology, AvailabilityMode availabilityMode,
                                          boolean totalOrder);

   /**
    * Broadcasts a write {@link ConsistentHash} update.
    */
   void broadcastWriteConsistentHashUpdate(String cacheName, CacheTopology cacheTopology, AvailabilityMode availabilityMode,
                                           boolean totalOrder);

   /**
    * Broadcasts an update for the current {@link CacheTopology}
    * <p>
    * It can be triggered when the topology needs an immediate update (like a node crashing/leaving)
    */
   void broadcastConsistentHashUpdate(String cacheName, CacheTopology cacheTopology, ConsistentHash newConsistentHash,
                                      AvailabilityMode availabilityMode, TopologyState state, boolean totalOrder);

   /**
    * Broadcasts the stable {@link CacheTopology}.
    */
   void broadcastStableTopologyUpdate(String cacheName, CacheTopology cacheTopology, boolean totalOrder);

   /**
    * Checks if the rebalance is enabled cluster-wide.
    * <p>
    * If it is disabled, the state transfer does not happen until it is enable again by invoking {@link
    * #setRebalancingEnabled(boolean)}.
    *
    * @return {@code true} if rebalance is enabled, {@code false} otherwise.
    */
   boolean isRebalancingEnabled();

   /**
    * Returns whether rebalancing is enabled or disabled for this container.
    */
   boolean isRebalancingEnabled(String cacheName);

   /**
    * Globally enables or disables whether automatic rebalancing should occur.
    */
   void setRebalancingEnabled(boolean enabled);

   /**
    * Enables or disables rebalancing for the specified cache
    */
   void setRebalancingEnabled(String cacheName, boolean enabled);

   /**
    * Retrieves the rebalancing status of a cache
    */
   RebalancingStatus getRebalancingStatus(String cacheName);

   /**
    * Forces a rebalance if there is one pending.
    * <p>
    * It does not depend if the rebalance is enabled or not.
    */
   void forceRebalance(String cacheName);

   /**
    * Forces an {@link AvailabilityMode} for the cluster.
    */
   void forceAvailabilityMode(String cacheName, AvailabilityMode availabilityMode);

}
