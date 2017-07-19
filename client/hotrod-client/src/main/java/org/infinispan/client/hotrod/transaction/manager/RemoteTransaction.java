package org.infinispan.client.hotrod.transaction.manager;

import org.infinispan.commons.tx.TransactionImpl;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
final class RemoteTransaction extends TransactionImpl {

   RemoteTransaction(RemoteTransactionManager transactionManager) {
      super();
      setXid(RemoteXid.create(transactionManager.getTransactionManagerId()));
   }

}
