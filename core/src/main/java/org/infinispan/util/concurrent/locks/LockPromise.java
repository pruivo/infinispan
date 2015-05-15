package org.infinispan.util.concurrent.locks;

import org.infinispan.util.concurrent.TimeoutException;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public interface LockPromise {

   /**
    * It tests if the lock is available.
    * <p/>
    * The lock is consider available when it is successfully acquired or the timeout is expired. In any case, when it
    * returns {@code true}, the {@link #lock()} will never block.
    *
    * @return {@code true} if the lock is available (or the timeout is expired), {@code false} otherwise.
    */
   boolean isAvailable();

   /**
    * It locks the key (or keys) associated to this promise.
    * <p/>
    * This method will block until the lock is available or the timeout is expired.
    *
    * @throws InterruptedException if the current thread is interrupted while acquiring the lock
    * @throws TimeoutException     if we are unable to acquire the lock after a specified timeout.
    */
   void lock() throws InterruptedException, TimeoutException;

}
