package org.infinispan.client.hotrod.impl.transaction;

import javax.transaction.Transaction;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public interface TransactionTable {

   <K, V> TransactionContext<K, V> enlist(TransactionalRemoteCache<K, V> txRemoteCache, Transaction tx);
}
