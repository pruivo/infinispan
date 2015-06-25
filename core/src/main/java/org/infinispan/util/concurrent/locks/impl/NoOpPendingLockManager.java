package org.infinispan.util.concurrent.locks.impl;

import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.util.concurrent.locks.PendingLockManager;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public class NoOpPendingLockManager implements PendingLockManager {

   private NoOpPendingLockManager() {
   }

   public static NoOpPendingLockManager getInstance() {
      return Wrapper.INSTANCE;
   }

   @Override
   public PendingLockPromise checkPendingForKey(TxInvocationContext<?> ctx, Object key, long time, TimeUnit unit) {
      return PendingLockPromise.NO_OP;
   }

   @Override
   public PendingLockPromise checkPendingForAllKeys(TxInvocationContext<?> ctx, Collection<Object> keys, long time, TimeUnit unit) {
      return PendingLockPromise.NO_OP;
   }

   @Override
   public long awaitPendingTransactionsForKey(TxInvocationContext<?> ctx, Object key, long time, TimeUnit unit) throws InterruptedException {
      return unit.toMillis(time);
   }

   @Override
   public long awaitPendingTransactionsForAllKeys(TxInvocationContext<?> ctx, Collection<Object> keys, long time, TimeUnit unit) throws InterruptedException {
      return unit.toMillis(time);
   }

   private static class Wrapper {
      private static final NoOpPendingLockManager INSTANCE = new NoOpPendingLockManager();
   }
}
