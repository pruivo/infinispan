package org.infinispan.interceptors.base;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.tx.TransactionBoundaryCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Interceptor that wraps the protocol dependent interceptors and redirects the transaction boundary commands (prepare,
 * commit and rollback) to the correct interceptor
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class ReconfigurableProtocolAwareWrapperInterceptor extends CommandInterceptor {

   private static final Log log = LogFactory.getLog(ReconfigurableProtocolAwareWrapperInterceptor.class);
   private final ConcurrentMap<String, CommandInterceptor> protocolDependentInterceptor = new ConcurrentHashMap<String, CommandInterceptor>();

   @Override
   public final Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      return handleTxBoundaryCommand(ctx, command);
   }

   @Override
   public final Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      return handleTxBoundaryCommand(ctx, command);
   }

   @Override
   public final Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      return handleTxBoundaryCommand(ctx, command);
   }

   @Override
   public void setNext(CommandInterceptor next) {
      super.setNext(next);
      for (CommandInterceptor ci : protocolDependentInterceptor.values()) {
         ci.setNext(next);
      }
   }

   /**
    * set the command interceptor that a transaction boundary command created in a specific transaction protocol should
    * visit
    *
    * @param protocolId  the transaction protocol identifier
    * @param interceptor the command interceptor
    */
   public final void setProtocolDependentInterceptor(String protocolId, CommandInterceptor interceptor) {
      protocolDependentInterceptor.put(protocolId, interceptor);
      interceptor.setNext(getNext());
      if (log.isTraceEnabled()) {
         log.tracef("Added a new replication protocol dependent interceptor. Interceptors are " + protocolDependentInterceptor);
      }
   }

   public final String routeTableToString() {
      return protocolDependentInterceptor.toString();
   }

   @Override
   protected Object handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
      return command.acceptVisitor(ctx, getNext(ctx.getProtocolId()));
   }

   private CommandInterceptor getNext(GlobalTransaction globalTransaction) {
      return getNext(globalTransaction.getProtocolId());
   }

   private CommandInterceptor getNext(String protocolId) {
      CommandInterceptor next = protocolDependentInterceptor.get(protocolId);
      if (log.isTraceEnabled()) {
         log.tracef("Executing command in protocol %s. Next interceptor is %s", protocolId, next);
      }
      return next != null ? next : getNext();
   }

   private Object handleTxBoundaryCommand(TxInvocationContext ctx, TransactionBoundaryCommand command) throws Throwable {
      return command.acceptVisitor(ctx, getNext(ctx.getGlobalTransaction()));
   }

}
