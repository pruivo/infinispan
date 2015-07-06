package org.infinispan.util.concurrent.locks;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public interface DeadlockChecker {

   boolean deadlockDetected(Object pendingOwner, Object currentOwner);

}
