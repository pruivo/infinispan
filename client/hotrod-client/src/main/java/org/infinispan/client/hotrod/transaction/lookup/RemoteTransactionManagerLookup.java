package org.infinispan.client.hotrod.transaction.lookup;

import javax.transaction.TransactionManager;

import org.infinispan.client.hotrod.transaction.manager.RemoteTransactionManager;
import org.infinispan.transaction.lookup.TransactionManagerLookup;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public class RemoteTransactionManagerLookup implements TransactionManagerLookup {

   private static final RemoteTransactionManagerLookup INSTANCE = new RemoteTransactionManagerLookup();

   private RemoteTransactionManagerLookup() {
   }

   public static TransactionManagerLookup getInstance() {
      return INSTANCE;
   }

   @Override
   public TransactionManager getTransactionManager() throws Exception {
      return RemoteTransactionManager.getInstance();
   }

   @Override
   public String toString() {
      return "DefaultTransactionManagerLookup{}";
   }
}
