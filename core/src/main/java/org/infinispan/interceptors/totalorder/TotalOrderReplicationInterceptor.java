package org.infinispan.interceptors.totalorder;

import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.ReplicationInterceptor;

/**
 * @author mircea.markus@jboss.com
 * @author Pedro Ruivo
 * @since 5.2.0
 */
public class TotalOrderReplicationInterceptor extends ReplicationInterceptor {

   @Override
   protected void broadcastPrepare(TxInvocationContext context, PrepareCommand command) {
      if (!context.isOriginLocal()) {
         throw new IllegalStateException("Expected a local context before TO-Broadcast prepare command");
      }
      try {
      totalOrderBroadcastPrepare(command, isSyncCommitPhase() ? null : getSelfDeliverFilter());
      } finally {
         totalOrderTxPrepare(context);
      }      
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      if (Configurations.isOnePhaseTotalOrderCommit(cacheConfiguration) || !shouldTotalOrderRollbackBeInvokeRemotely(ctx)) {
         return invokeNextInterceptor(ctx, command);
      }
      totalOrderTxRollback(ctx);
      return super.visitRollbackCommand(ctx, command);
   }



   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      if (Configurations.isOnePhaseTotalOrderCommit(cacheConfiguration)) {
         return invokeNextInterceptor(ctx, command);
      }
      totalOrderTxCommit(ctx);
      return super.visitCommitCommand(ctx, command);
   }
}
