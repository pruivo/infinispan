package org.infinispan.interceptors.gmu;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.GMUPrepareCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.remoting.responses.KeysValidateFilter;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.infinispan.transaction.gmu.GMUHelper.joinAndSetTransactionVersion;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 4.0
 */
public class TotalOrderGMUDistributionInterceptor extends GMUDistributionInterceptor {

   private static final Log log = LogFactory.getLog(TotalOrderGMUDistributionInterceptor.class);

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      //this map is only populated after locks are acquired. However, no locks are acquired when total order is enabled
      //so we need to populate it here
      if (ctx.isOriginLocal() && !ctx.getCacheTransaction().hasModification(ClearCommand.class)) {
         Set<Object> affectedKeys = new HashSet<Object>();
         affectedKeys.addAll(Arrays.asList(command.getAffectedKeysToLock(false)));
         affectedKeys.addAll(ctx.getReadSet());
         ctx.addAllAffectedKeys(affectedKeys);
      }
      return super.visitPrepareCommand(ctx, command);
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      if (Configurations.isOnePhaseTotalOrderCommit(cacheConfiguration) || !shouldTotalOrderRollbackBeInvokedRemotely(ctx)) {
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

   @Override
   protected void prepareOnAffectedNodes(TxInvocationContext ctx, PrepareCommand command, Collection<Address> recipients, boolean sync) {
      if (log.isTraceEnabled()) {
         log.tracef("Total Order Anycast transaction %s with Total Order", command.getGlobalTransaction().globalId());
      }

      if (!ctx.isOriginLocal()) {
         throw new IllegalStateException("Expected a local context while TO-Anycast prepare command");
      }

      if (!(command instanceof GMUPrepareCommand)) {
         throw new IllegalStateException("Expected a Versioned Prepare Command in version aware component");
      }

      try {
         Set<Object> affectedKeys = ctx.getAffectedKeys();

         ResponseFilter responseFilter = affectedKeys.isEmpty() || isSyncCommitPhase() ? null :
               new KeysValidateFilter(rpcManager.getAddress(), affectedKeys);

         Map<Address, Response> responseMap = totalOrderAnycastPrepare(recipients, command, responseFilter);
         joinAndSetTransactionVersion(responseMap.values(), ctx, versionGenerator);
      } finally {
         transactionRemotelyPrepared(ctx);
      }
   }

   @Override
   protected void lockAndWrap(InvocationContext ctx, Object key, InternalCacheEntry ice, FlagAffectedCommand command) throws InterruptedException {
      entryFactory.wrapEntryForPut(ctx, key, ice, false, command);
   }
}
