package org.infinispan.stats.container.transactional;

import org.infinispan.stats.container.DataAccessStatisticsContainer;
import org.infinispan.stats.container.LockStatisticsContainer;
import org.infinispan.stats.container.NetworkStatisticsContainer;

/**
 * //TODO: document this!
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
public class LocalTxStatisticsContainer implements DataAccessStatisticsContainer, LockStatisticsContainer,
                                                   TransactionStatisticsContainer, NetworkStatisticsContainer {

   @Override
   public void writeAccess(long duration) {
      //To change body of implemented methods use File | Settings | File Templates.
   }

   @Override
   public void writeAccessLockTimeout(long duration) {
      //To change body of implemented methods use File | Settings | File Templates.
   }

   @Override
   public void writeAccessNetworkTimeout(long duration) {
      //To change body of implemented methods use File | Settings | File Templates.
   }

   @Override
   public void writeAccessDeadlock(long duration) {
      //To change body of implemented methods use File | Settings | File Templates.
   }

   @Override
   public void writeAccessValidationError(long duration) {
      //To change body of implemented methods use File | Settings | File Templates.
   }

   @Override
   public void writeAccessUnknownError(long duration) {
      //To change body of implemented methods use File | Settings | File Templates.
   }

   @Override
   public void readAccess(long duration) {
      //To change body of implemented methods use File | Settings | File Templates.
   }

   @Override
   public void readAccessLockTimeout(long duration) {
      //To change body of implemented methods use File | Settings | File Templates.
   }

   @Override
   public void readAccessNetworkTimeout(long duration) {
      //To change body of implemented methods use File | Settings | File Templates.
   }

   @Override
   public void readAccessDeadlock(long duration) {
      //To change body of implemented methods use File | Settings | File Templates.
   }

   @Override
   public void readAccessValidationError(long duration) {
      //To change body of implemented methods use File | Settings | File Templates.
   }

   @Override
   public void readAccessUnknownError(long duration) {
      //To change body of implemented methods use File | Settings | File Templates.
   }

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

   @Override
   public void remoteGet(long rtt, int size) {
      //To change body of implemented methods use File | Settings | File Templates.
   }

   @Override
   public void prepare(long rtt, int size) {
      //To change body of implemented methods use File | Settings | File Templates.
   }

   @Override
   public void commit(long rtt, int size) {
      //To change body of implemented methods use File | Settings | File Templates.
   }

   @Override
   public void rollback(long rtt, int size) {
      //To change body of implemented methods use File | Settings | File Templates.
   }
}
