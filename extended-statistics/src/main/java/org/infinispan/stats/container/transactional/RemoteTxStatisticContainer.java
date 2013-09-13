package org.infinispan.stats.container.transactional;

import org.infinispan.stats.container.LockStatisticsContainer;

/**
 * //TODO: document this!
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
public class RemoteTxStatisticContainer implements LockStatisticsContainer, TransactionStatisticsContainer {

   @Override
   public void markLocksAcquired() {
      //To change body of implemented methods use File | Settings | File Templates.
   }

   @Override
   public void addLockTimeout(long waitingTime) {
      //To change body of implemented methods use File | Settings | File Templates.
   }

   @Override
   public void addDeadlock(long waitingTime) {
      //To change body of implemented methods use File | Settings | File Templates.
   }

   @Override
   public void addLock(long waitingTime, long holdTime) {
      //To change body of implemented methods use File | Settings | File Templates.
   }

   @Override
   public void endExecution() {
      //To change body of implemented methods use File | Settings | File Templates.
   }

   @Override
   public boolean isFinished() {
      return false;  //To change body of implemented methods use File | Settings | File Templates.
   }

   @Override
   public void prepare(long duration, boolean onePhaseCommit) {
      //To change body of implemented methods use File | Settings | File Templates.
   }

   @Override
   public void prepareLockTimeout(long duration, boolean onePhaseCommit) {
      //To change body of implemented methods use File | Settings | File Templates.
   }

   @Override
   public void prepareNetworkTimeout(long duration, boolean onePhaseCommit) {
      //To change body of implemented methods use File | Settings | File Templates.
   }

   @Override
   public void prepareDeadlockError(long duration, boolean onePhaseCommit) {
      //To change body of implemented methods use File | Settings | File Templates.
   }

   @Override
   public void prepareValidationError(long duration, boolean onePhaseCommit) {
      //To change body of implemented methods use File | Settings | File Templates.
   }

   @Override
   public void prepareUnknownError(long duration, boolean onePhaseCommit) {
      //To change body of implemented methods use File | Settings | File Templates.
   }

   @Override
   public void commit(long duration) {
      //To change body of implemented methods use File | Settings | File Templates.
   }

   @Override
   public void commitNetworkTimeout(long duration) {
      //To change body of implemented methods use File | Settings | File Templates.
   }

   @Override
   public void commitUnknownError(long duration) {
      //To change body of implemented methods use File | Settings | File Templates.
   }

   @Override
   public void rollback(long duration) {
      //To change body of implemented methods use File | Settings | File Templates.
   }

   @Override
   public void rollbackNetworkTimeout(long duration) {
      //To change body of implemented methods use File | Settings | File Templates.
   }

   @Override
   public void rollbackUnknownError(long duration) {
      //To change body of implemented methods use File | Settings | File Templates.
   }
}
