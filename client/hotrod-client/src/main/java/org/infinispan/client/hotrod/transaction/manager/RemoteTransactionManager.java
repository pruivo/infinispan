package org.infinispan.client.hotrod.transaction.manager;

import java.util.UUID;

import javax.transaction.Transaction;

import org.infinispan.commons.tx.TransactionManagerImpl;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public final class RemoteTransactionManager extends TransactionManagerImpl {

   private RemoteTransactionManager() {
      super();
   }

   public static RemoteTransactionManager getInstance() {
      return LazyInitializeHolder.INSTANCE;
   }

   @Override
   protected Transaction createTransaction() {
      return new RemoteTransaction(this);
   }

   UUID getTransactionManagerId() {
      return transactionManagerId;
   }

   private static class LazyInitializeHolder {
      static final RemoteTransactionManager INSTANCE = new RemoteTransactionManager();
   }
}
