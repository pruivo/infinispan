package org.infinispan.util.concurrent.locks.containers.wrappers;

import java.util.concurrent.TimeUnit;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
public interface LockWrapper {
   LockHolder add(Object key, Object lockOwner);

   Object getOwner();

   boolean isLocked();

   boolean tryLock(Object lockOwner, long timeout, TimeUnit unit) throws InterruptedException;

   void lock(Object lockOwner);

   void unlock(Object owner);

   void remove(LockHolder lockHolder);

   boolean isEmpty();
}
