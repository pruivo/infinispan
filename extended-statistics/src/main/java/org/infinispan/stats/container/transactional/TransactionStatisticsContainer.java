package org.infinispan.stats.container.transactional;

/**
 * //TODO: document this!
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
public interface TransactionStatisticsContainer {

   void markFinished();

   boolean isFinished();

   void prepare(long duration);

   void prepareLockTimeout(long duration);

   void prepareNetworkTimeout(long duration);

   void prepareDeadlockError(long duration);

   void prepareValidationError(long duration);

   void prepareUnknownError(long duration);

   void commit(long duration);

   void commitNetworkTimeout(long duration);

   void commitUnknownError(long duration);

   void rollback(long duration);

   void rollbackNetworkTimeout(long duration);

   void rollbackUnknownError(long duration);

}
