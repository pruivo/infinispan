package org.infinispan.stats.container;

/**
 * //TODO: document this!
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
public class NonTxLockStatistcsContainer implements LockStatisticsContainer {

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
}
