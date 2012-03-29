package org.infinispan.interceptors;

import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.*;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.remoting.rpc.RpcManager;

/**
 * This interceptor has the behavior needed for passive replication, namely, it now allow the processing of write 
 * operations / transactions in non-primary node (i.e. in backup nodes)
 *
 * The Passive Replication protocol was developed by Diego Didona and Sebastiano Peluso
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class PassiveReplicationInterceptor extends CommandInterceptor {

   private RpcManager rpcManager;

   @Inject
   public void inject(RpcManager rpcManager) {
      this.rpcManager = rpcManager;
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitApplyDeltaCommand(InvocationContext ctx, ApplyDeltaCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitEvictCommand(InvocationContext ctx, EvictCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitInvalidateCommand(InvocationContext ctx, InvalidateCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitInvalidateL1Command(InvocationContext ctx, InvalidateL1Command command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      if (ctx.isOriginLocal() && !isMaster() && (ctx.hasModifications() || command.hasModifications())) {
         throw new IllegalStateException("Write transaction not allowed in Passive Replication, in backup node");
      }
      return invokeNextInterceptor(ctx, command);
   }

   private Object handleWriteCommand(InvocationContext ctx, WriteCommand command) throws Throwable {
      if (ctx.isOriginLocal() && !isMaster()) {
         throw new IllegalStateException("Write operation not allowed in Passive Replication, in backup nodes");
      }
      return invokeNextInterceptor(ctx, command);
   }

   private boolean isMaster() {
      return rpcManager.getTransport().isCoordinator();
   }
}
