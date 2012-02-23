package org.infinispan.interceptors.totalorder;

import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.distribution.VersionedDistributionInterceptor;
import org.infinispan.remoting.responses.KeysValidateFilter;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.infinispan.transaction.WriteSkewHelper.setVersionsSeenOnPrepareCommand;

/**
 * This interceptor is used in total order in distributed mode when the write skew check is enabled.
 * After sending the prepare through TOA (Total Order Anycast), it blocks the execution thread until the transaction
 * outcome is know (i.e., the write skew check passes in all keys owners)
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class TotalOrderVersionedDistributionInterceptor extends VersionedDistributionInterceptor {

   private static final Log log = LogFactory.getLog(TotalOrderVersionedDistributionInterceptor.class);

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      //this map is only populated after locks are acquired. However, no locks are acquired when total order is enabled
      //so we need to populate it here
      ctx.addAllAffectedKeys(Util.getAffectedKeys(Arrays.asList(command.getModifications()), dataContainer));
      return super.visitPrepareCommand(ctx, command);
   }

   @Override
   protected void prepareOnAffectedNodes(TxInvocationContext ctx, PrepareCommand command, Collection<Address> recipients, boolean sync) {
      if(log.isTraceEnabled()) {
         log.tracef("Total Order Anycast transaction %s with Total Order", command.getGlobalTransaction().prettyPrint());
      }

      if (!ctx.isOriginLocal()) {
         throw new IllegalStateException("Expected a local context while TO-Anycast prepare command");
      }

      if (!(command instanceof VersionedPrepareCommand)) {
         throw new IllegalStateException("Expected a Versioned Prepare Command in version aware component");
      }

      try {
         Set<Object> affectedKeys = getAffectedKeys(command);

         ResponseFilter responseFilter = affectedKeys == null || isSyncCommitPhase() ? null :
               new KeysValidateFilter(rpcManager.getAddress(), affectedKeys);

         setVersionsSeenOnPrepareCommand((VersionedPrepareCommand) command, ctx);
         totalOrderAnycastPrepare(recipients, command, sync, responseFilter);
      } finally {
         totalOrderTxPrepare(ctx);
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

   private Set<Object> getAffectedKeys(PrepareCommand prepareCommand) {
      Set<Object> affectedKeys = new HashSet<Object>();
      for (WriteCommand writeCommand : prepareCommand.getModifications()) {
         if (writeCommand instanceof ClearCommand) {
            return null;
         } else {
            affectedKeys.addAll(writeCommand.getAffectedKeys());
         }
      }
      return affectedKeys;
   }
}
