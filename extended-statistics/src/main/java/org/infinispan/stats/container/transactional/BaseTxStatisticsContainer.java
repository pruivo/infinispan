package org.infinispan.stats.container.transactional;

import org.infinispan.stats.container.LockStatisticsContainer;
import org.infinispan.util.TimeService;

import java.util.HashMap;
import java.util.Map;

import static org.infinispan.stats.container.transactional.TxOutcome.*;

/**
 * //TODO: document this!
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
public abstract class BaseTxStatisticsContainer implements LockStatisticsContainer, TransactionStatisticsContainer {

   protected final LockStats lockStats;
   protected final TxStats txStats;
   protected final TimeService timeService;
   private final Map<Object, LockInfo> lockInfoMap;
   private State state;

   protected BaseTxStatisticsContainer(TimeService timeService, long startRunning) {
      this.timeService = timeService;
      this.state = State.RUNNING;
      lockInfoMap = new HashMap<Object, LockInfo>();
      lockStats = new LockStats();
      txStats = new TxStats(startRunning);
   }

   @Override
   public void keyLocked(Object key, long waitingTime) {
      if (lockInfoMap.containsKey(key)) {
         return;
      }
      LockInfo info = new LockInfo();
      info.waitingTime = waitingTime;
      lockInfoMap.put(key, info);
   }

   @Override
   public void keyUnlocked(Object key) {
      LockInfo info = lockInfoMap.remove(key);
      addLockAcquired(info);
   }

   @Override
   public void lockTimeout(long waitingTime) {
      addLockWaitingTime(waitingTime);
      lockStats.lockTimeout++;
   }

   @Override
   public void deadlock(long waitingTime) {
      addLockWaitingTime(waitingTime);
      lockStats.lockDeadlock++;
   }

   @Override
   public void markFinished() {
      if (txStats.endCommittingTimeStamp <= 0) {
         txStats.endCommittingTimeStamp = timeService.time();
      }
      state = State.FINISHED;
   }

   @Override
   public final boolean isFinished() {
      return state == State.FINISHED && lockInfoMap.isEmpty();
   }

   @Override
   public final void prepare(long duration) {
      prepared(SUCCESS, duration);
   }

   @Override
   public final void prepareLockTimeout(long duration) {
      prepared(TxOutcome.LOCK_TIMEOUT, duration);
   }

   @Override
   public final void prepareNetworkTimeout(long duration) {
      prepared(NETWORK_TIMEOUT, duration);
   }

   @Override
   public final void prepareDeadlockError(long duration) {
      prepared(TxOutcome.DEADLOCK, duration);
   }

   @Override
   public final void prepareValidationError(long duration) {
      prepared(TxOutcome.VALIDATION, duration);
   }

   @Override
   public final void prepareUnknownError(long duration) {
      prepared(UNKNOWN_ERROR, duration);
   }

   @Override
   public final void commit(long duration) {
      terminate(true, duration, SUCCESS);
   }

   @Override
   public final void commitNetworkTimeout(long duration) {
      terminate(true, duration, NETWORK_TIMEOUT);
   }

   @Override
   public final void commitUnknownError(long duration) {
      terminate(true, duration, UNKNOWN_ERROR);
   }

   @Override
   public final void rollback(long duration) {
      terminate(false, duration, UNKNOWN_ERROR);
   }

   @Override
   public final void rollbackNetworkTimeout(long duration) {
      terminate(false, duration, NETWORK_TIMEOUT);
   }

   @Override
   public final void rollbackUnknownError(long duration) {
      terminate(false, duration, UNKNOWN_ERROR);
   }

   private void endExecution() {
      if (state == State.RUNNING) {
         if (txStats.endRunningTimeStamp <= 0) {
            txStats.endRunningTimeStamp = timeService.time();
         }
         state = State.COMMITTING;
      }
   }

   private void addLockAcquired(LockInfo info) {
      addLockWaitingTime(info.waitingTime);
      lockStats.locksAcquired++;
      lockStats.holdTime += (timeService.time() - info.lockTimeStamp);
   }

   private synchronized void prepared(TxOutcome outcome, long duration) {
      endExecution();
      txStats.prepareCommandDuration = duration;
      txStats.updateOutcome(outcome);
   }

   private synchronized void terminate(boolean commit, long duration, TxOutcome outcome) {
      endExecution();
      if (commit) {
         txStats.commitCommandDuration = duration;
      } else {
         txStats.rollbackCommandDuration = duration;
      }
      txStats.updateOutcome(outcome);
   }

   private void addLockWaitingTime(long waiting) {
      if (waiting > 0) {
         lockStats.lockWaited++;
         lockStats.waitingTime += waiting;
      }
   }

   private enum State {
      RUNNING, COMMITTING, FINISHED
   }

   protected class LockStats {
      protected int locksAcquired;
      protected long holdTime;
      protected int lockWaited;
      protected long waitingTime;
      protected int lockTimeout;
      protected int lockDeadlock;
   }

   protected class TxStats {
      protected final long startRunningTimeStamp;
      protected TxOutcome outcome;
      protected long endRunningTimeStamp;
      protected long endCommittingTimeStamp;
      protected long prepareCommandDuration;
      protected long commitCommandDuration;
      protected long rollbackCommandDuration;

      private TxStats(long startRunningTimeStamp) {
         this.startRunningTimeStamp = startRunningTimeStamp;
      }

      private void updateOutcome(TxOutcome outcome) {
         if (this.outcome == null) {
            this.outcome = outcome;
         } else if (this.outcome == SUCCESS && outcome != SUCCESS) {
            this.outcome = outcome;
         } else if (this.outcome == UNKNOWN_ERROR && outcome != SUCCESS) {
            this.outcome = outcome;
         }
      }
   }

   private class LockInfo {
      private final long lockTimeStamp = timeService.time();
      private long waitingTime = -1;
   }
}
