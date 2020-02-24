package org.infinispan.interceptors.impl;

import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 10.1
 */
public class OptimisticTxIracLocalInterceptor extends AbstractIracInterceptor {

   private static final Log log = LogFactory.getLog(OptimisticTxIracLocalInterceptor.class);
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
         return onLocalCommitCommand(asLocalTxInvocationContext(ctx), command);
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


}
