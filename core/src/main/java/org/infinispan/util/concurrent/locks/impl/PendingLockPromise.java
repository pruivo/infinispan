package org.infinispan.util.concurrent.locks.impl;

import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.locks.LockPromise;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public interface PendingLockPromise extends LockPromise {


   long getRemainingTimeout();

}
