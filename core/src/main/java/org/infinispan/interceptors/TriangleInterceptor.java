package org.infinispan.interceptors;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.write.BackupAckCommand;
import org.infinispan.commands.write.BackupWriteCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.concurrent.CommandAckCollector;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class TriangleInterceptor extends DDAsyncInterceptor {

   private static final Log log = LogFactory.getLog(TriangleInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();

   private RpcManager rpcManager;
   private CommandsFactory commandsFactory;
   private CommandAckCollector commandAckCollector;

   private Address localAddress;
   private long timeoutNanos;

   @Inject
   public void inject(RpcManager rpcManager, CommandsFactory commandsFactory,
                      CommandAckCollector commandAckCollector, DistributionManager distributionManager) {
      this.rpcManager = rpcManager;
      this.commandsFactory = commandsFactory;
      this.commandAckCollector = commandAckCollector;
   }

   @Start
   public void start() {
      localAddress = rpcManager.getAddress();
      RpcOptions options = rpcManager.getDefaultRpcOptions(true);
      timeoutNanos = options.timeUnit().toNanos(options.timeout());
   }

   @Override
   public BasicInvocationStage visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return handleWriteCommands(ctx, command);
   }

   @Override
   public BasicInvocationStage visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return handleWriteCommands(ctx, command);
   }

   @Override
   public BasicInvocationStage visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return handleWriteCommands(ctx, command);
   }

   @Override
   public BasicInvocationStage visitBackupWriteCommand(InvocationContext ctx, BackupWriteCommand command) throws Throwable {
      return invokeNext(ctx, command).handle(this::onBackupCommand);
   }

   private BasicInvocationStage handleWriteCommands(InvocationContext ctx, VisitableCommand command) throws Throwable {
      return invokeNext(ctx, command).thenApply(this::onWriteCommand);
   }

   private Object onWriteCommand(InvocationContext rCtx, VisitableCommand rCommand, Object rv) throws Throwable {
      final DataWriteCommand cmd = (DataWriteCommand) rCommand;
      final CommandInvocationId id = cmd.getCommandInvocationId();
      if (rCtx.isOriginLocal()) {
         if (trace) {
            log.tracef("Waiting for acks for command %s.", id);
         }
         return commandAckCollector.awaitCollector(id, timeoutNanos, TimeUnit.NANOSECONDS, cmd);
      }
      return rv;
   }

   private void onBackupCommand(InvocationContext rCtx, VisitableCommand rCommand, Object rv, Throwable throwable) throws Throwable {
      BackupWriteCommand cmd = (BackupWriteCommand) rCommand;
      if (cmd.shouldSendAck()) {
         sendAck(cmd);
      }
   }

   private void sendAck(BackupWriteCommand command) {
      final CommandInvocationId id = command.getCommandInvocationId();
      final Address origin = id.getAddress();
      if (trace) {
         log.tracef("Sending acks for command %s. Originator=%s.", id, origin);
      }
      if (origin.equals(localAddress)) {
         commandAckCollector.ack(id, command.getPreviousValue());
      } else {
         rpcManager.sendTo(origin, createAck(id, command.getPreviousValue()), DeliverOrder.NONE);
      }
   }

   private BackupAckCommand createAck(CommandInvocationId id, Object previousValue) {
      return commandsFactory.buildBackupAckCommand(id, previousValue);
   }

   public enum KeyOwnership {
      PRIMARY, BACKUP, NONE;

      public static KeyOwnership ownership(List<Address> owners, Address localNode) {
         Iterator<Address> iterator = owners.iterator();
         if (localNode.equals(iterator.next())) {
            return PRIMARY;
         }
         while (iterator.hasNext()) {
            if (localNode.equals(iterator.next())) {
               return BACKUP;
            }
         }
         return NONE;
      }
   }
}
