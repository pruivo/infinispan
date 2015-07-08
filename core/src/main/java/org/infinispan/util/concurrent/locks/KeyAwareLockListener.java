package org.infinispan.util.concurrent.locks;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public interface KeyAwareLockListener {
   void onEvent(Object key, LockState state);
}
