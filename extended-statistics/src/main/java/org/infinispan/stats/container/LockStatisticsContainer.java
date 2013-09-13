package org.infinispan.stats.container;

/**
 * //TODO: document this!
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
public interface LockStatisticsContainer {

   void markLocksAcquired();

   void addLockTimeout(long waitingTime);

   void addDeadlock(long waitingTime);

   void addLock(long waitingTime, long holdTime);

}
