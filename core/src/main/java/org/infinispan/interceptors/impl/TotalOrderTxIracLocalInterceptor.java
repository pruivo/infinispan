package org.infinispan.interceptors.impl;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 10.1
 */
@Deprecated
public class TotalOrderTxIracLocalInterceptor extends AbstractIracInterceptor {

   private static final Log log = LogFactory.getLog(TotalOrderTxIracLocalInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      final Object key = command.getKey();
      if (isIracState(command)) {
         setMetadataToCacheEntry(ctx.lookupEntry(key), getIracMetadataFromCommand(command, key));
      }
      return invokeNext(ctx, command);
   }

   @SuppressWarnings("rawtypes")
   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      if (command.isOnePhaseCommit()) {
         return invokeNextThenAccept(ctx, command, this::afterOnePhasePrepare);
      }
      if (ctx.isOriginLocal()) {
         return invokeNextThenAccept(ctx, command, this::afterLocalTwoPhasePrepare);
      } else {
         return invokeNextThenApply(ctx, command, this::afterRemoteTwoPhasePrepare);
      }
   }

   @SuppressWarnings("rawtypes")
   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      if (ctx.isOriginLocal()) {
         return onLocalCommitCommand(ctx, command);
      } else {
         return onRemoteCommitCommand(ctx, command);
      }
   }

   @SuppressWarnings("rawtypes")
   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      //nothing extra to be done for rollback.
      return invokeNext(ctx, command);
   }

   @Override
   boolean isTraceEnabled() {
      return trace;
   }

   @Override
   boolean isDebugEnabled() {
      return log.isDebugEnabled();
   }

   @Override
   Log getLog() {
      return log;
   }

   private void afterOnePhasePrepare(InvocationContext ctx, PrepareCommand command, Object rv) {
      Iterator<StreamData> iterator = streamKeysFromModifications(command.getModifications())
            .filter(this::isWriteOwner)
            .distinct()
            .iterator();
      Map<Integer, IracMetadata> segmentMetadata = new HashMap<>();
      while (iterator.hasNext()) {
         StreamData data = iterator.next();
         IracMetadata metadata = segmentMetadata
               .computeIfAbsent(data.segment, iracVersionGenerator::generateNewMetadata);
         setMetadataToCacheEntry(ctx.lookupEntry(data.key), metadata);
      }
   }
}
