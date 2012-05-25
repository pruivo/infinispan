package org.infinispan.distribution.wrappers;

import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.remote.recovery.TxCompletionNotificationCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.TransactionBoundaryCommand;
import org.infinispan.remoting.InboundInvocationHandler;
import org.infinispan.remoting.transport.Address;
import org.infinispan.stats.TransactionsStatisticsRegistry;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import static org.infinispan.stats.translations.ExposedStatistics.IspnStats;

/**
 * @author Mircea Markus <mircea.markus@jboss.com> (C) 2011 Red Hat Inc.
 * @author Pedro Ruivo
 * @since 5.2
 */
public class InboundInvocationHandlerWrapper implements InboundInvocationHandler {

   private final InboundInvocationHandler actual;
   private static final Log log = LogFactory.getLog(InboundInvocationHandlerWrapper.class);

   private final TransactionTable transactionTable;

   public InboundInvocationHandlerWrapper(InboundInvocationHandler actual, TransactionTable transactionTable) {
      this.actual = actual;
      this.transactionTable = transactionTable;
   }

   @Override
   public Object handle(CacheRpcCommand command, Address origin) throws Throwable {
      if (log.isTraceEnabled()) {
         log.tracef("Handle remote command [%s] by the invocation handle wrapper from %s", command, origin);
      }
      GlobalTransaction globalTransaction = getGlobalTransaction(command);
      try{
         if (globalTransaction != null) {
            if (log.isDebugEnabled()) {
               log.debugf("The command %s is transactional and the global transaction is %s", command,
                          globalTransaction.prettyPrint());
            }
            TransactionsStatisticsRegistry.attachRemoteTransactionStatistic(globalTransaction, command instanceof PrepareCommand ||
                  command instanceof CommitCommand);
         } else {
            if (log.isDebugEnabled()) {
               log.debugf("The command %s is NOT transactional", command);
            }
         }

         boolean txCompleteNotify = command instanceof TxCompletionNotificationCommand;
         long currTime = 0;
         if (txCompleteNotify) {
            currTime = System.nanoTime();
         }

         Object ret = actual.handle(command,origin);

         if (txCompleteNotify) {
            TransactionsStatisticsRegistry.addValueAndFlushIfNeeded(IspnStats.TX_COMPLETE_NOTIFY_EXECUTION_TIME,
                                                                    System.nanoTime() - currTime, false);
            TransactionsStatisticsRegistry.incrementValueAndFlushIfNeeded(IspnStats.NUM_TX_COMPLETE_NOTIFY_COMMAND, false);
         }

         return ret;
      } finally {
         if (globalTransaction != null) {
            if (log.isDebugEnabled()) {
               log.debugf("Detach statistics for command %s", command, globalTransaction.prettyPrint());
            }
            TransactionsStatisticsRegistry.detachRemoteTransactionStatistic(globalTransaction,
                                                                            !transactionTable.containRemoteTx(globalTransaction));
         }
      }
   }

   private GlobalTransaction getGlobalTransaction(CacheRpcCommand cacheRpcCommand) {
      if (cacheRpcCommand instanceof TransactionBoundaryCommand) {
         return ((TransactionBoundaryCommand) cacheRpcCommand).getGlobalTransaction();
      } else if (cacheRpcCommand instanceof TxCompletionNotificationCommand) {
         for (Object obj : cacheRpcCommand.getParameters()) {
            if (obj instanceof GlobalTransaction) {
               return (GlobalTransaction) obj;
            }
         }
      }
      return null;
   }
}
