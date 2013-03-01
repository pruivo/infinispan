package org.infinispan.dataplacement.statetransfer;

import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.OutboundTransferTask;
import org.infinispan.statetransfer.StateProviderImpl;
import org.infinispan.topology.CacheTopology;

import java.util.Set;

/**
 * The State Provider logic that is aware of the Data Placement optimization and the keys that can be moved in each
 * segment
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class DataPlacementStateProvider extends StateProviderImpl {

   @Override
   protected OutboundTransferTask createTask(Address destination, Set<Integer> segments, CacheTopology cacheTopology) {
      return new DataPlacementOutboundTransferTask(destination, segments, chunkSize, cacheTopology.getTopologyId(),
                                                   cacheTopology.getReadConsistentHash(), this,
                                                   cacheTopology.getWriteConsistentHash(), dataContainer,
                                                   cacheLoaderManager, rpcManager, commandsFactory, timeout, cacheName);
   }

   @Override
   protected boolean isKeyLocal(Object key, ConsistentHash consistentHash, Set<Integer> segments) {
      //a key is local to this node is it belongs to the segments requested and it has not been moved
      boolean isLocal = consistentHash.isKeyLocalToNode(rpcManager.getAddress(), key);
      boolean belongToSegments = segments == null || segments.contains(consistentHash.getSegment(key));
      return isLocal && belongToSegments;
   }
}
