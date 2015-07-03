package org.infinispan.util.concurrent.locks;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public enum LockState {
   WAITING,
   AVAILABLE,
   ACQUIRED,
   TIMED_OUT,
   DEADLOCKED,
   RELEASED
}
