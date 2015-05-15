package org.infinispan.util.concurrent.locks;

import org.infinispan.util.concurrent.locks.impl.InfinispanLock;

/**
 * A container for locks
 *
 * @author Manik Surtani
 * @author Mircea.Markus@jboss.com
 * @author Pedro Ruivo
 * @since 4.0
 */
public interface LockContainerV8 {

   /**
    * @param key the key to lock.
    * @return the lock for a specific object to be acquired. If the lock does not exists, it is created.
    */
   InfinispanLock get(Object key);

   /**
    * @param key the key to lock.
    * @return the lock for a specific object. If the lock does not exists, it return {@code null}.
    */
   InfinispanLock peek(Object key);

   /**
    * @return number of locks held
    */
   int getNumLocksHeld();

   /**
    * @return the size of the shared lock pool
    */
   int size();
}
