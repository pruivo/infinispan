package org.infinispan.interceptors.impl;

import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.distribution.Ownership;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 10.1
 */
public class NonTxIracRemoteInterceptor extends AbstractIracRemoteInterceptor {

   private static final Log log = LogFactory.getLog(NonTxIracRemoteInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return visitDataWriteCommand(ctx, command);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return visitDataWriteCommand(ctx, command);
   }

   @Override
   boolean isTraceEnabled() {
      return trace;
   }

   @Override
   Log getLog() {
      return log;
   }

   private Object visitDataWriteCommand(InvocationContext ctx, DataWriteCommand command) {
      final Object key = command.getKey();
      if (skipCommand(command)) {
         return invokeNext(ctx, command);
      }

      Ownership ownership = getOwnership(key);

      switch (ownership) {
         case PRIMARY:
            //we are on primary and the lock is acquired
            //if the update is discarded, command.isSuccessful() will return false.
            return invokeNextThenAccept(ctx, command, this::validateOnPrimary);
         case BACKUP:
            if (!ctx.isOriginLocal()) {
               //backups only commit when the command are remote (i.e. after validated from the originator)
               return invokeNextThenAccept(ctx, command, this::setIracMetadataForOwner);
            }
      }
      return invokeNext(ctx, command);
   }

   private boolean skipCommand(DataWriteCommand command) {
      return !command.hasAnyFlag(FlagBitSets.IRAC_UPDATE); //normal operation
   }
}
