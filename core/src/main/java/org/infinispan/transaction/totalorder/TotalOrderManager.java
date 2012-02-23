package org.infinispan.transaction.totalorder;

import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.statetransfer.TransactionInfo;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.RemoteTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;

import java.util.Collection;

/**
 * This class is responsible to validate transactions in the total order based protocol. It ensures the delivered order
 * and will validate multiple transactions in parallel if they are non conflicting transaction.
 *
 * @author Pedro Ruivo
 * @author mircea.markus@jboss.com
 * @since 5.2.0
 */
public interface TotalOrderManager {

   /**
    * Processes the transaction as received from the sequencer.
    */
   void processTransactionFromSequencer(PrepareCommand prepareCommand, TxInvocationContext ctx, CommandInterceptor invoker) throws Exception;

   /**
    * This will mark a global transaction as finished. It will be invoked in the processing of the commit command in
    * repeatable read with write skew (not implemented yet!)
    */
   void finishTransaction(RemoteTransaction transaction, boolean commit);

   void addTransactions(Collection<TransactionInfo> pendingTransactions);

   void notifyTransactionTransferStart();

   void notifyTransactionTransferEnd();
}
