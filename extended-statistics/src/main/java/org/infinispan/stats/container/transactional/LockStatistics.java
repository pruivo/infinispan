package org.infinispan.stats.container.transactional;

/**
 * Lock statistics used in transactional and non-transactional caches.
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
public enum LockStatistics {

   //LOCK
   NUM_LOCK,
   LOCK_HOLD_DUR,
   NUM_LOCK_WAIT,
   LOCK_WAIT_DUR,
   NUM_LOCK_FAIL_TIMEOUT,
   NUM_LOCK_FAIL_DEADLOCK,

}
