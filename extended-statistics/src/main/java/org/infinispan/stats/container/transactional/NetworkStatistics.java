package org.infinispan.stats.container.transactional;

/**
 * Network related statistics. Used in transactional and non-transactional caches.
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
public enum NetworkStatistics {
   //remote get
   NUM_GET,
   GET_SIZE,
   GET_RTT,
   GET_NUM_NODES,

   //remote prepare
   NUM_PREPARE,
   PREPARE_SIZE,
   PREPARE_RTT,
   PREPARE_NUM_NODES,

   //remote commit
   NUM_COMMIT,
   COMMIT_SIZE,
   COMMIT_RTT,
   COMMIT_NUM_NODES,

   //remote rollback
   NUM_ROLLBACK,
   ROLLBACK_SIZE,
   ROLLBACK_RTT,
   ROLLBACK_NUM_NODES,
}
