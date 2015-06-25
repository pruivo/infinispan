package org.infinispan.util.concurrent.locks;

import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.util.concurrent.locks.impl.PendingLockPromise;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public interface PendingLockManager {
   PendingLockPromise checkPendingForKey(TxInvocationContext<?> ctx, Object key, long time, TimeUnit unit);

   PendingLockPromise checkPendingForAllKeys(TxInvocationContext<?> ctx, Collection<Object> keys, long time, TimeUnit unit);

   long awaitPendingTransactionsForKey(TxInvocationContext<?> ctx, Object key, long time, TimeUnit unit)
         throws InterruptedException;

   long awaitPendingTransactionsForAllKeys(TxInvocationContext<?> ctx, Collection<Object> keys, long time, TimeUnit unit)
         throws InterruptedException;
}
