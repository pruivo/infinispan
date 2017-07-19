package org.infinispan.client.hotrod.impl.transaction;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Consumer;

import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

import org.infinispan.client.hotrod.impl.operations.CompleteTransactionOperation;
import org.infinispan.client.hotrod.impl.operations.PrepareTransactionOperation;
import org.infinispan.client.hotrod.impl.transaction.entry.Modification;
import org.infinispan.client.hotrod.transaction.manager.RemoteXid;
import org.infinispan.commons.CacheException;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public class SyncModeTransactionTable implements TransactionTable {

   private final Map<Transaction, SynchronizationAdapter> registeredTransactions = new ConcurrentHashMap<>();
   private final UUID uuid = UUID.randomUUID();

   @Override
   public <K, V> TransactionContext<K, V> enlist(TransactionalRemoteCache<K, V> txRemoteCache, Transaction tx) {
      SynchronizationAdapter adapter = registeredTransactions.computeIfAbsent(tx, this::createSynchronizationAdapter);
      return adapter.registerCache(txRemoteCache);
   }

   private SynchronizationAdapter createSynchronizationAdapter(Transaction transaction) {
      SynchronizationAdapter adapter = new SynchronizationAdapter(transaction, registeredTransactions::remove, RemoteXid.create(uuid));
      try {
         transaction.registerSynchronization(adapter);
      } catch (RollbackException | SystemException e) {
         throw new CacheException(e);
      }
      return adapter;
   }

   private static class SynchronizationAdapter implements Synchronization {

      private final Map<String, TransactionContext<?, ?>> registeredCaches = new ConcurrentSkipListMap<>();
      private final Collection<TransactionContext<?, ?>> preparedCaches = new LinkedList<>();
      private final Transaction transaction;
      private final Consumer<Transaction> cleanupTask;
      private final RemoteXid xid;

      private SynchronizationAdapter(Transaction transaction, Consumer<Transaction> cleanupTask, RemoteXid xid) {
         this.transaction = transaction;
         this.cleanupTask = cleanupTask;
         this.xid = xid;
      }

      @Override
      public void beforeCompletion() {
         if (isMarkedRollback()) {
            return;
         }
         for (TransactionContext<?, ?> txContext : registeredCaches.values()) {
            switch (prepareContext(txContext)) {
               case XAResource.XA_OK:
                  preparedCaches.add(txContext);
                  break;
               case XAResource.XA_RDONLY:
                  break; //read only tx.
               case Integer.MIN_VALUE:
                  //signals a marshaller error of key or value. the server wasn't contacted
                  markAsRollback();
                  return;
               default: //any other code we need to rollback
                  //we may need to send the rollback later
                  preparedCaches.add(txContext);
                  markAsRollback();
                  return;
            }
         }
      }

      @Override
      public void afterCompletion(int status) {
         if (status == Status.STATUS_COMMITTED) {
            preparedCaches.forEach(this::commitContext);
         } else {
            preparedCaches.forEach(this::rollbackContext);
         }
         cleanupTask.accept(transaction);
      }

      private void rollbackContext(TransactionContext<?, ?> context) {
         try {
            CompleteTransactionOperation operation = context.getOperationsFactory()
                  .newCompleteTransactionOperation(null, false);
            operation.execute();
         } catch (Exception e) {
            e.printStackTrace();
         }
      }

      private void commitContext(TransactionContext<?, ?> context) {
         try {
            CompleteTransactionOperation operation = context.getOperationsFactory()
                  .newCompleteTransactionOperation(null, true);
            operation.execute();
         } catch (Exception e) {
            e.printStackTrace();
         }
      }

      private void markAsRollback() {
         try {
            transaction.setRollbackOnly();
         } catch (SystemException e) {
            e.printStackTrace();  // TODO: Customise this generated block
         }
      }

      private int prepareContext(TransactionContext<?, ?> context) {
         PrepareTransactionOperation operation;
         try {
            Collection<Modification> modifications = context.toModification();
            if (modifications.isEmpty()) {
               return XAResource.XA_RDONLY;
            }
            operation = context.getOperationsFactory().newPrepareTransactionOperation(xid, false, modifications);
         } catch (Exception e) {
            return Integer.MIN_VALUE;
         }
         try {
            int xaReturnCode;
            do {
               xaReturnCode = operation.execute();
            } while (operation.shouldRetry());
            return xaReturnCode;
         } catch (Exception e) {
            return XAException.XA_RBROLLBACK;
         }
      }

      private boolean isMarkedRollback() {
         try {
            return transaction.getStatus() == Status.STATUS_MARKED_ROLLBACK;
         } catch (SystemException e) {
            //lets assume not.
            return false;
         }
      }

      private <K, V> TransactionContext<K, V> registerCache(TransactionalRemoteCache<K, V> txRemoteCache) {
         //noinspection unchecked
         return (TransactionContext<K, V>) registeredCaches
               .computeIfAbsent(txRemoteCache.getName(), s -> new TransactionContext<>(
                     txRemoteCache.keyMarshaller(), txRemoteCache.valueMarshaller(),
                     txRemoteCache.getOperationsFactory(), txRemoteCache.getName()));
      }
   }
}
