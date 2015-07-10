package org.infinispan.util.concurrent.locks;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public interface ExtendedLockPromise extends LockPromise {

   void cancel(LockState cause);

   Object getRequestor();

   Object getOwner();

}
