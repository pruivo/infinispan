package org.infinispan.stats.container;

import java.util.Collection;

/**
 * //TODO: document this!
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
public class NonTxLockStatistcsContainer implements LockStatisticsContainer {


   @Override
   public void keyLocked(Object key, long waitingTime) {
      //To change body of implemented methods use File | Settings | File Templates.
   }

   @Override
   public void keyUnlocked(Object key) {
      //To change body of implemented methods use File | Settings | File Templates.
   }

   @Override
   public void keysUnlocked(Collection<Object> key) {
      //To change body of implemented methods use File | Settings | File Templates.
   }

   @Override
   public void lockTimeout(long waitingTime) {
      //To change body of implemented methods use File | Settings | File Templates.
   }

   @Override
   public void deadlock(long waitingTime) {
      //To change body of implemented methods use File | Settings | File Templates.
   }
}
