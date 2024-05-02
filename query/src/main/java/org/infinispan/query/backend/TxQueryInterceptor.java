package org.infinispan.query.backend;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.interceptors.InvocationSuccessFunction;
import org.infinispan.telemetry.InfinispanTelemetry;
import org.infinispan.telemetry.SpanCategory;
import org.infinispan.telemetry.impl.DisabledInfinispanSpan;
import org.infinispan.transaction.impl.AbstractCacheTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;

public final class TxQueryInterceptor extends DDAsyncInterceptor {

   @Inject InfinispanTelemetry telemetry;

   private final ConcurrentMap<GlobalTransaction, Map<Object, Object>> txOldValues;

   // Storing direct reference from one interceptor to another is antipattern, but extracting helper
   // wouldn't help much.
   private final QueryInterceptor queryInterceptor;

   private final InvocationSuccessFunction<VisitableCommand> commitModificationsToIndex = this::commitModificationsToIndexFuture;

   public TxQueryInterceptor(ConcurrentMap<GlobalTransaction, Map<Object, Object>> txOldValues, QueryInterceptor queryInterceptor) {
      this.txOldValues = txOldValues;
      this.queryInterceptor = queryInterceptor;
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) {
      if (command.isOnePhaseCommit()) {
         return invokeNextThenApply(ctx, command, commitModificationsToIndex);
      } else {
         return invokeNext(ctx, command);
      }
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) {
      return invokeNextThenApply(ctx, command, commitModificationsToIndex);
   }

   private Object commitModificationsToIndexFuture(InvocationContext ctx, VisitableCommand cmd, Object rv) {
      TxInvocationContext<?> txCtx = (TxInvocationContext<?>) ctx;
      Map<Object, Object> removed = txOldValues.remove(txCtx.getGlobalTransaction());
      final Map<Object, Object> oldValues = removed == null ? Collections.emptyMap() : removed;

      AbstractCacheTransaction transaction = txCtx.getCacheTransaction();
      var stage = CompletionStages.aggregateCompletionStage();
      var traceData = cmd.getTraceCommandData();
      var span = DisabledInfinispanSpan.instance();
      if (traceData != null) {
         span = telemetry.startTraceRequest("index", traceData.attributes(SpanCategory.CONTAINER), traceData.context());
      }
      transaction.getAllModifications().stream()
            .filter(mod -> !mod.hasAnyFlag(FlagBitSets.SKIP_INDEXING))
            .flatMap(mod -> mod.getAffectedKeys().stream())
            .forEach(key -> {
               CacheEntry<?, ?> entry = txCtx.lookupEntry(key);
               if (entry != null) {
                  Object oldValue = oldValues.getOrDefault(key, QueryInterceptor.UNKNOWN);
                  stage.dependsOn(queryInterceptor.processChange(ctx, null, key, oldValue, entry.getValue()));
               }

            });

      return delayedValue(stage.freeze().whenComplete(span), rv);
   }
}
