package org.infinispan.stats.container.transactional;

import org.infinispan.stats.container.EnumStatisticsContainer;
import org.infinispan.stats.container.LockStatisticsContainer;
import org.infinispan.util.TimeService;

/**
 * //TODO: document this!
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
public abstract class BaseTxStatisticsContainer implements LockStatisticsContainer, TransactionStatisticsContainer {

   protected final EnumStatisticsContainer<LockStatistics> lockStatistics;
   protected final EnumStatisticsContainer<TransactionStatistics> transactionStatistics;
   private final TimeService timeService;
   private final long startRunning;
   private long endRunning;
   private long endTransaction;
   private State state;
   private boolean hasLocks;

   protected BaseTxStatisticsContainer(TimeService timeService, long startRunning) {
      this.timeService = timeService;
      this.lockStatistics = new EnumStatisticsContainer<LockStatistics>(LockStatistics.class);
      this.transactionStatistics = new EnumStatisticsContainer<TransactionStatistics>(TransactionStatistics.class);
      this.state = State.RUNNING;
      this.hasLocks = false;
      this.startRunning = startRunning;
   }

   @Override
   public final void notifyLockAcquired() {
      this.hasLocks = true;
   }

   @Override
   public final void addLockTimeout(long waitingTime) {
      addLockWaitingTime(waitingTime);
      lockStatistics.increment(LockStatistics.NUM_LOCK_FAIL_TIMEOUT);
   }

   @Override
   public final void addDeadlock(long waitingTime) {
      addLockWaitingTime(waitingTime);
      lockStatistics.increment(LockStatistics.NUM_LOCK_FAIL_DEADLOCK);
   }

   @Override
   public final void addLock(long waitingTime, long holdTime) {
      addLockWaitingTime(waitingTime);
      lockStatistics.increment(LockStatistics.NUM_LOCK);
      lockStatistics.add(LockStatistics.LOCK_HOLD_DUR, holdTime);
   }

   @Override
   public final void endExecution() {
      if (state == State.RUNNING) {
         if (endRunning <= 0) {
            endRunning = timeService.time();
         }
         if (startRunning > 0) {
            transactionStatistics.add(TransactionStatistics.TX_EXECUTION_DUR, startRunning - endRunning);
         }
         state = State.COMMITTING;
      }
   }

   @Override
   public final boolean isFinished() {
      return state == State.FINISHED && !hasLocks;
   }

   @Override
   public final void prepare(long duration, boolean onePhaseCommit) {
      prepared(TransactionStatistics.NUM_PREPARE, TransactionStatistics.PREPARE_DUR, duration, onePhaseCommit);
   }

   @Override
   public final void prepareLockTimeout(long duration, boolean onePhaseCommit) {
      prepared(TransactionStatistics.NUM_PREPARE_FAIL_LOCK_TIMEOUT, TransactionStatistics.PREPARE_DUR_FAIL_LOCK_TIMEOUT,
               duration, onePhaseCommit);
   }

   @Override
   public final void prepareNetworkTimeout(long duration, boolean onePhaseCommit) {
      prepared(TransactionStatistics.NUM_PREPARE_FAIL_NETWORK_TIMEOUT,
               TransactionStatistics.PREPARE_DUR_FAIL_NETWORK_TIMEOUT, duration, onePhaseCommit);
   }

   @Override
   public final void prepareDeadlockError(long duration, boolean onePhaseCommit) {
      prepared(TransactionStatistics.NUM_PREPARE_FAIL_DEADLOCK, TransactionStatistics.PREPARE_DUR_FAIL_DEADLOCK,
               duration, onePhaseCommit);
   }

   @Override
   public final void prepareValidationError(long duration, boolean onePhaseCommit) {
      prepared(TransactionStatistics.NUM_PREPARE_FAIL_VALIDATION, TransactionStatistics.PREPARE_DUR_FAIL_VALIDATION,
               duration, onePhaseCommit);
   }

   @Override
   public final void prepareUnknownError(long duration, boolean onePhaseCommit) {
      prepared(TransactionStatistics.NUM_PREPARE_FAIL_UNKNOWN, TransactionStatistics.PREPARE_DUR_FAIL_UNKNOWN, duration,
               onePhaseCommit);
   }

   @Override
   public final void commit(long duration) {
      committedOrRollbacked(TransactionStatistics.NUM_COMMIT, TransactionStatistics.COMMIT_DUR, duration);
   }

   @Override
   public final void commitNetworkTimeout(long duration) {
      committedOrRollbacked(TransactionStatistics.NUM_COMMIT_FAIL_NETWORK_TIMEOUT,
                            TransactionStatistics.COMMIT_DUR_FAIL_NETWORK_TIMEOUT, duration);
   }

   @Override
   public final void commitUnknownError(long duration) {
      committedOrRollbacked(TransactionStatistics.NUM_COMMIT_FAIL_UNKNOWN, TransactionStatistics.COMMIT_DUR_FAIL_UNKNOWN,
                            duration);
   }

   @Override
   public final void rollback(long duration) {
      committedOrRollbacked(TransactionStatistics.NUM_ROLLBACK, TransactionStatistics.ROLLBACK_DUR, duration);
   }

   @Override
   public final void rollbackNetworkTimeout(long duration) {
      committedOrRollbacked(TransactionStatistics.NUM_ROLLBACK_FAIL_NETWORK_TIMEOUT,
                            TransactionStatistics.ROLLBACK_DUR_FAIL_NETWORK_TIMEOUT, duration);
   }

   @Override
   public final void rollbackUnknownError(long duration) {
      committedOrRollbacked(TransactionStatistics.NUM_ROLLBACK_FAIL_UNKNOWN,
                            TransactionStatistics.ROLLBACK_DUR_FAIL_UNKNOWN, duration);
   }

   private void prepared(TransactionStatistics counterStats, TransactionStatistics durationStats, long duration,
                         boolean onePhaseCommit) {
      state = onePhaseCommit ? State.FINISHED : State.COMMITTING;
      transactionStatistics.add(durationStats, duration);
      transactionStatistics.increment(counterStats);
      if (onePhaseCommit) {
         checkFinished();
      }
   }

   private void committedOrRollbacked(TransactionStatistics counterStats, TransactionStatistics durationStats,
                                      long duration) {
      state = State.FINISHED;
      transactionStatistics.add(durationStats, duration);
      transactionStatistics.increment(counterStats);
      checkFinished();
   }

   private void checkFinished() {
      if (isFinished() && endTransaction <= 0) {
         endTransaction = timeService.time();
         transactionStatistics.add(TransactionStatistics.TX_TERMINATION_DUR, endTransaction - endRunning);
      }
   }

   private void addLockWaitingTime(long waiting) {
      lockStatistics.add(LockStatistics.LOCK_WAIT_DUR, waiting);
      lockStatistics.increment(LockStatistics.NUM_LOCK_WAIT);
   }

   private enum State {
      RUNNING,
      COMMITTING,
      FINISHED
   }
}
