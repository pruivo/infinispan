package org.infinispan.stats.container.transactional;

/**
 * //TODO: document this!
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
public interface TransactionStatisticsContainer {

   void endExecution();

   boolean isFinished();

   void prepare(long duration, boolean onePhaseCommit);

   void prepareLockTimeout(long duration, boolean onePhaseCommit);

   void prepareNetworkTimeout(long duration, boolean onePhaseCommit);

   void prepareDeadlockError(long duration, boolean onePhaseCommit);

   void prepareValidationError(long duration, boolean onePhaseCommit);

   void prepareUnknownError(long duration, boolean onePhaseCommit);

   void commit(long duration);

   void commitNetworkTimeout(long duration);

   void commitUnknownError(long duration);

   void rollback(long duration);

   void rollbackNetworkTimeout(long duration);

   void rollbackUnknownError(long duration);

}
