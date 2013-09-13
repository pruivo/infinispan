package org.infinispan.stats.container.transactional;

/**
 * //TODO: document this!
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
public enum TransactionStatistics {

   //PREPARE
   NUM_PREPARE,
   PREPARE_DUR,
   NUM_PREPARE_FAIL_LOCK_TIMEOUT,
   PREPARE_DUR_FAIL_LOCK_TIMEOUT,
   NUM_PREPARE_FAIL_NETWORK_TIMEOUT,
   PREPARE_DUR_FAIL_NETWORK_TIMEOUT,
   NUM_PREPARE_FAIL_DEADLOCK,
   PREPARE_DUR_FAIL_DEADLOCK,
   NUM_PREPARE_FAIL_VALIDATION,
   PREPARE_DUR_FAIL_VALIDATION,
   NUM_PREPARE_FAIL_UNKNOWN,
   PREPARE_DUR_FAIL_UNKNOWN,

   //COMMIT
   NUM_COMMIT,
   COMMIT_DUR,
   NUM_COMMIT_FAIL_NETWORK_TIMEOUT,
   COMMIT_DUR_FAIL_NETWORK_TIMEOUT,
   NUM_COMMIT_FAIL_UNKNOWN,
   COMMIT_DUR_FAIL_UNKNOWN,

   //ROLLBACK
   NUM_ROLLBACK,
   ROLLBACK_DUR,
   NUM_ROLLBACK_FAIL_NETWORK_TIMEOUT,
   ROLLBACK_DUR_FAIL_NETWORK_TIMEOUT,
   NUM_ROLLBACK_FAIL_UNKNOWN,
   ROLLBACK_DUR_FAIL_UNKNOWN,

   //Transaction only
   TX_EXECUTION_DUR,
   TX_TERMINATION_DUR,
}
