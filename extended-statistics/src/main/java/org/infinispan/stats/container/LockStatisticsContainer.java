package org.infinispan.stats.container;

import java.util.Collection;

/**
 * //TODO: document this!
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
public interface LockStatisticsContainer {

   void keyLocked(Object key, long waitingTime);

   void keyUnlocked(Object key);

   void keysUnlocked(Collection<Object> key);

   void lockTimeout(long waitingTime);

   void deadlock(long waitingTime);

}
