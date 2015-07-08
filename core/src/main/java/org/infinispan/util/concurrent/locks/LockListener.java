package org.infinispan.util.concurrent.locks;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public interface LockListener {

   /**
    * Invoked when the lock is available.
    *
    * @param state the current lock state. It can be {@link LockState#AVAILABLE}, {@link LockState#TIMED_OUT} or
    *              {@link LockState#DEADLOCKED}.
    */
   void onEvent(LockState state);
}
