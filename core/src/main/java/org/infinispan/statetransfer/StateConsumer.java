package org.infinispan.statetransfer;

import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.CacheTopology;

import java.util.Collection;

/**
 * Handles inbound state transfers.
 *
 * @author anistor@redhat.com
 * @author Pedro Ruivo
 * @since 5.2
 */
@Scope(Scopes.NAMED_CACHE)
public interface StateConsumer {

   /**
    * @return the current{@link CacheTopology}.
    */
   CacheTopology getCacheTopology();

   /**
    * @return {@code true} if the state transfer is in progress.
    */
   boolean isStateTransferInProgress();

   /**
    * @return {@code true} if the state transfer is in progress and the key will be transfer to this node.
    */
   boolean isStateTransferInProgressForKey(Object key);

   /**
    * Starts a state transfer by requesting the state to previous owners.
    */
   void onRebalanceStart(CacheTopology cacheTopology);

   /**
    * Finishes the state transfer and updates the read consistent hash.
    */
   void onReadConsistentHashUpdate(CacheTopology cacheTopology);

   /**
    * Updates the write consistent hash and removes keys no longer owned by this node.
    */
   void onWriteConsistentHashUpdate(CacheTopology cacheTopology);

   /**
    * Updates the read and write consistent hash.
    *
    * If it is received during the state transfer, it should restart it if necessary.
    */
   void onConsistentHashUpdate(CacheTopology cacheTopology);

   /**
    * It applies some state from another node (a previous owner).
    */
   void applyState(Address sender, int topologyId, Collection<StateChunk> stateChunks);

   /**
    * Cancels all incoming state transfers. The already received data is not discarded.
    * This is executed when the cache is shutting down.
    */
   void stop();

   /**
    * Stops applying incoming state. Also stops tracking updated keys. Should be called at the end of state transfer or
    * when a ClearCommand is committed during state transfer.
    */
   void stopApplyingState();

   /**
    * @return  true if this node has already received the first rebalance command
    */
   boolean ownsData();
}
