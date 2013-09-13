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

   //remote prepare
   NUM_PREPARE,
   PREPARE_SIZE,
   PREPARE_RTT,

   //remote commit
   NUM_COMMIT,
   COMMIT_SIZE,
   COMMIT_RTT,

   //remote rollback
   NUM_ROLLBACK,
   ROLLBACK_SIZE,
   ROLLBACK_RTT,
}
