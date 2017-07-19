package org.infinispan.client.hotrod.transaction.lookup;

import java.util.Optional;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.transaction.TransactionManager;

import org.infinispan.client.hotrod.transaction.manager.RemoteTransactionManager;
import org.infinispan.commons.util.Util;
import org.infinispan.transaction.lookup.LookupNames;
import org.infinispan.transaction.lookup.TransactionManagerLookup;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public class GenericTransactionManagerLookup implements TransactionManagerLookup {

   private static final GenericTransactionManagerLookup INSTANCE = new GenericTransactionManagerLookup();
   private TransactionManager transactionManager = null;

   private GenericTransactionManagerLookup() {
   }

   public static GenericTransactionManagerLookup getInstance() {
      return INSTANCE;
   }

   @Override
   public synchronized TransactionManager getTransactionManager() throws Exception {
      if (transactionManager != null) {
         return transactionManager;
      }

      transactionManager = tryLookup().orElseGet(RemoteTransactionManager::getInstance);
      return transactionManager;
   }

   private Optional<TransactionManager> tryLookup() {
      InitialContext ctx;
      try {
         ctx = new InitialContext();
      } catch (NamingException e) {
         return Optional.empty();
      }

      try {
         //probe jndi lookups first
         for (LookupNames.JndiTransactionManager knownJNDIManager : LookupNames.JndiTransactionManager.values()) {
            Object jndiObject;
            try {
               jndiObject = ctx.lookup(knownJNDIManager.getJndiLookup());
            } catch (NamingException e) {
               continue;
            }
            if (jndiObject instanceof TransactionManager) {
               return Optional.of((TransactionManager) jndiObject);
            }
         }
      } finally {
         Util.close(ctx);
      }

      for (LookupNames.TransactionManagerFactory transactionManagerFactory : LookupNames.TransactionManagerFactory.values()) {
         TransactionManager transactionManager = transactionManagerFactory.tryLookup(GenericTransactionManagerLookup.class.getClassLoader());
         if (transactionManager != null) {
            return Optional.of(transactionManager);
         }
      }
      return Optional.empty();
   }
}
