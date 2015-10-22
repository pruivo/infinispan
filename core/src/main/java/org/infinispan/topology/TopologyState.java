package org.infinispan.topology;

/**
 * The different states when a cluster topology changes.
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public enum TopologyState {
   /**
    * The state transfer is in progress.
    */
   REBALANCE,
   /**
    * The state transfer is finished and the nodes are updating the read consistent hash.
    */
   READ_CH_UPDATE,
   /**
    * The topology is stable, i.e. no state transfer.
    */
   NONE
}
