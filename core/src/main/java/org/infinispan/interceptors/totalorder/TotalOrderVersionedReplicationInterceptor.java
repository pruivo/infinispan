package org.infinispan.interceptors.totalorder;

import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.VersionedReplicationInterceptor;

import static org.infinispan.transaction.WriteSkewHelper.setVersionsSeenOnPrepareCommand;

/**
 * Replication Interceptor for Total Order protocol with versioning.
 *
 * @author Pedro Ruivo
 * @author Mircea.Markus@jboss.com
 * @since 5.2
 */
public class TotalOrderVersionedReplicationInterceptor extends VersionedReplicationInterceptor {

   @Override
   protected void broadcastPrepare(TxInvocationContext context, PrepareCommand command) {
      if (!context.isOriginLocal()) {
         throw new IllegalStateException("Expected a local context before TO-Broadcast prepare command");
      }

      if (!(command instanceof VersionedPrepareCommand)) {
         throw new IllegalStateException("Expected a Versioned Prepare Command in version aware component");
      }

      setVersionsSeenOnPrepareCommand((VersionedPrepareCommand) command, context);
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
