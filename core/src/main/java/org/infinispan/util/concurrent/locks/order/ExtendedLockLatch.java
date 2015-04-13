package org.infinispan.util.concurrent.locks.order;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public interface ExtendedLockLatch extends LockLatch {
   void markReady();
}
