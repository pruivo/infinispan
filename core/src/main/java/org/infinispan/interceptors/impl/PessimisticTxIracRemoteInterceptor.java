package org.infinispan.interceptors.impl;

import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 10.1
 */
public class PessimisticTxIracRemoteInterceptor extends AbstractIracRemoteInterceptor {

   private static final Log log = LogFactory.getLog(PessimisticTxIracRemoteInterceptor.class);
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
   boolean isDebugEnabled() {
      return log.isDebugEnabled();
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

      DistributionInfo dInfo = getDistributionInfo(key);
      //this is a "limitation" with pessimistic transactions
      //the tx is committed in a single phase so the SiteMaster needs to forward the update to the primary owner
      //then, the tx will run locally on the primary owner and it can be validated
      if (ctx.isOriginLocal()) {
         if (dInfo.isPrimary()) {
            validateOnPrimary(ctx, command, null);
            //at this point, the lock is acquired.
            //If the update is discarded (command.isSuccessful() == false) it will not be enlisted in the tx
            //the tx will be read-only and a successful ack will be sent to the original site.
            return command.isSuccessful() ? invokeNext(ctx, command) : null;
         } else {
            throw new CacheException("Update must be executed in the primary owner!", null, false, false);
         }
      } else {
         if (dInfo.isWriteOwner()) {
            setIracMetadataForOwner(ctx, command, null);
         }
      }

      return invokeNext(ctx, command);
   }

   private boolean skipCommand(DataWriteCommand command) {
      return !command.hasAnyFlag(FlagBitSets.IRAC_UPDATE); //normal operation
   }
}
