package org.infinispan.interceptors.totalorder;

import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.tx.AbstractTransactionBoundaryCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.RemoteTransaction;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.totalorder.TotalOrderManager;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jgroups.blocks.RequestHandler;

/**
 * Created to control the total order validation. It disable the possibility of acquiring locks during execution through
 * the cache API
 *
 * @author Pedro Ruivo
 * @author Mircea.Markus@jboss.com
 * @since 5.2
 */
public class TotalOrderInterceptor extends CommandInterceptor {

   private static final Log log = LogFactory.getLog(TotalOrderInterceptor.class);
   private boolean trace;

   private TotalOrderManager totalOrderManager;
   private TransactionTable transactionTable;

   @Inject
   public void inject(TotalOrderManager totalOrderManager, TransactionTable transactionTable) {
      this.totalOrderManager = totalOrderManager;
      this.transactionTable = transactionTable;
   }

   @Start
   public void setLogLevel() {
      this.trace = log.isTraceEnabled();
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      if (trace) {
         log.tracef("Prepare received. Transaction=%s, Affected keys=%s, Local=%s",
                    command.getGlobalTransaction().prettyPrint(),
                    command.getAffectedKeys(),
                    ctx.isOriginLocal());
      }

      try {
         if (ctx.isOriginLocal()) {
            return invokeNextInterceptor(ctx, command);
         } else {
            totalOrderManager.processTransactionFromSequencer(command, ctx, getNext());
            return RequestHandler.DO_NOT_REPLY;
         }
      } catch (Throwable exception) {
         if (log.isDebugEnabled()) {
            log.debugf(exception, "Exception while preparing for transaction %s. Local=%s",
                       command.getGlobalTransaction().prettyPrint());
         }
         throw exception;
      }
   }

   @Override
   public Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command) throws Throwable {
      throw new UnsupportedOperationException("Lock interface not supported with total order protocol");
   }

   //The rollback and commit command are only invoked with repeatable read + write skew + versioning

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      GlobalTransaction gtx = command.getGlobalTransaction();
      if (trace) {
         log.tracef("Rollback received. Transaction=%s, Local=%s", gtx.prettyPrint(), ctx.isOriginLocal());
      }

      RemoteTransaction remoteTransaction = transactionTable.getRemoteTransaction(gtx);

      try {
         if (!shouldProcessCommand(remoteTransaction, command)) {
            return null;
         }

         return invokeNextInterceptor(ctx, command);
      } catch (Throwable exception) {
         if (log.isDebugEnabled()) {
            log.debugf(exception, "Exception while rollbacking transaction %s", gtx.prettyPrint());
         }
         throw exception;
      } finally {
         if (remoteTransaction != null && remoteTransaction.isFinished()) {
            totalOrderManager.finishTransaction(remoteTransaction, false);
         }
      }
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      GlobalTransaction gtx = command.getGlobalTransaction();

      if (trace) {
         log.tracef("Commit received. Transaction=%s",  gtx.prettyPrint());
      }

      RemoteTransaction remoteTransaction = transactionTable.getRemoteTransaction(gtx);

      try {
         if (!shouldProcessCommand(remoteTransaction, command)) {
            return null;
         }

         return invokeNextInterceptor(ctx, command);
      } catch (Throwable exception) {
         if (log.isDebugEnabled()) {
            log.debugf(exception, "Exception while committing transaction %s", gtx.prettyPrint());
         }
         throw exception;
      } finally {
         if (remoteTransaction != null && remoteTransaction.isFinished()) {
            totalOrderManager.finishTransaction(remoteTransaction, true);
         }
      }
   }

   /**
    * in total order, the commit/rollback (C/R) commands are not ordered with the prepare (P).
    * so it is possible to deliver the C/R before the P.
    *
    * This method checks if the P is already received. if true, the C/R must be processed normally. otherwise, we can
    * avoid processing it and mark the remote transaction as committed or rollbacked
    *
    * @param remoteTransaction   the remote transaction
    * @param command             the commit or rollback command
    * @return                    true if the command must be processed through the interceptor chain, false otherwise
    */
   private boolean shouldProcessCommand(RemoteTransaction remoteTransaction, AbstractTransactionBoundaryCommand command) {
      if (remoteTransaction == null) {
         return true;
      }

      boolean commit = command instanceof CommitCommand;

      try {
         return remoteTransaction.waitUntilPrepared(commit);
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         log.timeoutWaitingUntilTransactionPrepared(remoteTransaction.getGlobalTransaction().prettyPrint());
      }
      return false;
   }
}
