package org.infinispan.stats.container.transactional;

/**
 * //TODO: document this!
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
public enum TxOutcome {
   SUCCESS,
   LOCK_TIMEOUT,
   NETWORK_TIMEOUT,
   DEADLOCK,
   VALIDATION,
   UNKNOWN_ERROR
}
