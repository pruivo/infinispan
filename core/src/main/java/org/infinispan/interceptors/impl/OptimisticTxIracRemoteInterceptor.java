package org.infinispan.interceptors.impl;

import java.util.List;

import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 10.1
 */
public class OptimisticTxIracRemoteInterceptor extends AbstractIracRemoteInterceptor {

   private static final Log log = LogFactory.getLog(OptimisticTxIracRemoteInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      if (skipCommand(command.getModifications())) {
         return invokeNext(ctx, command);
      }
      DataWriteCommand cmd = (DataWriteCommand) command.getModifications()[0];
      if (getDistributionInfo(cmd.getKey()).isPrimary()) {
         validateOnPrimary(ctx, cmd, null);
         //this works for now.
         //but when we introduce the custom conflict resolution we need to send back the new version to the originator... somehow
         if (!cmd.isSuccessful()) {
            throw new CacheException("Discard Update", null, true, false);
         }
      }
      return invokeNext(ctx, command);
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      if (skipCommand(ctx.getModifications())) {
         return invokeNext(ctx, command);
      }
      setIracMetadataForOwner(ctx, (DataWriteCommand) ctx.getModifications().get(0), null);
      return invokeNext(ctx, command);
   }

   @Override
   boolean isTraceEnabled() {
      return trace;
   }

   @Override
   Log getLog() {
      return log;
   }

   private boolean skipCommand(List<WriteCommand> mods) {
      //we shouldn't have IRAC_UPDATE commands with normal commands
      return mods.size() != 1 || !mods.get(0).hasAnyFlag(FlagBitSets.IRAC_UPDATE);
   }

   private boolean skipCommand(WriteCommand[] mods) {
      //we shouldn't have IRAC_UPDATE commands with normal commands
      return mods.length != 1 || !mods[0].hasAnyFlag(FlagBitSets.IRAC_UPDATE);
   }
}
