package org.infinispan.interceptors;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.write.BackupAckCommand;
import org.infinispan.commands.write.BackupWriteCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.responses.UnsuccessfulResponse;
import org.infinispan.remoting.responses.UnsuccessfulWithValueResponse;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.concurrent.CommandAckCollector;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class TriangleInterceptor extends DDAsyncInterceptor {

   private static final Log log = LogFactory.getLog(TriangleInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();

   private DistributionManager distributionManager;
   private RpcManager rpcManager;
   private CommandsFactory commandsFactory;
   private CommandAckCollector commandAckCollector;

   private RpcOptions asyncRpcOptions;
   private Address localAddress;

   private static Response createResponse(Object returnValue, WriteCommand command) {
      final boolean isSuccessful = command.isSuccessful();
      final boolean skipReturnValue = returnValue == null || !command.isReturnValueExpected();
      if (isSuccessful) {
         return skipReturnValue ? null : SuccessfulResponse.create(returnValue);
      } else {
         return skipReturnValue ? UnsuccessfulResponse.INSTANCE : UnsuccessfulWithValueResponse.create(returnValue);
      }
   }

   @Inject
   public void inject(RpcManager rpcManager, CommandsFactory commandsFactory,
                      CommandAckCollector commandAckCollector, DistributionManager distributionManager) {
      this.rpcManager = rpcManager;
      this.commandsFactory = commandsFactory;
      this.commandAckCollector = commandAckCollector;
      this.distributionManager = distributionManager;
   }

   @Start
   public void start() {
      asyncRpcOptions = rpcManager.getDefaultRpcOptions(false, DeliverOrder.NONE);
      localAddress = rpcManager.getAddress();
   }

   @Override
   public CompletableFuture<Void> visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return handleWriteCommands(ctx);
   }

   @Override
   public CompletableFuture<Void> visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return handleWriteCommands(ctx);
   }

   @Override
   public CompletableFuture<Void> visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return handleWriteCommands(ctx);
   }

   @Override
   public Object visitBackupWriteCommand(InvocationContext ctx, BackupWriteCommand command) throws Throwable {
      return ctx.onReturn((rCtx, rCommand, rv, throwable) -> {
         BackupWriteCommand cmd = (BackupWriteCommand) rCommand;
         //remote only. we need to send the ack even with exceptions
         sendAck(cmd.getCommandInvocationId(), getPrimaryOwner(cmd.getKey()));
         return null;
      });
   }

   private CompletableFuture<Void> handleWriteCommands(InvocationContext ctx) throws Throwable {
      return ctx.onReturn((rCtx, rCommand, rv, throwable) -> {
         final DataWriteCommand cmd = (DataWriteCommand) rCommand;
         final Object key = cmd.getKey();
         final KeyOwnership keyOwnership = getKeyOwnership(key);
         final CommandInvocationId id = cmd.getCommandInvocationId();

         /*if (keyOwnership == KeyOwnership.BACKUP) {
            //send acks back. we are the originator
            sendAck(id, getPrimaryOwner(key));
         }*/
         if (throwable != null) {
            return null; //don't change return value
         }
         if (rCtx.isOriginLocal()) {
            if (trace) {
               log.tracef("Waiting for acks for command %s.", id);
            }
            commandAckCollector.awaitCollector(id, asyncRpcOptions.timeout(), asyncRpcOptions.timeUnit());
            return null;
         }

         if (trace) {
            log.tracef("Processing remote command %s. Ownership=%s", cmd, keyOwnership);
         }

         switch (keyOwnership) {
            case PRIMARY:
               Response rsp = createResponse(rv, cmd);
               if (trace) {
                  log.tracef("Primary Owner response=%s", rsp);
               }
               return CompletableFuture.completedFuture(rsp);
            case BACKUP:
               //remotely, it does not need to send anything back to primary
               return null;
            default:
               throw new IllegalStateException("Unknown key ownership " + keyOwnership);
         }
      });
   }

   private void sendAck(CommandInvocationId id, Address primaryOwner) {
      final Address origin = id.getAddress();
      if (trace) {
         log.tracef("Sending acks for command %s. PrimaryOwner=%s. Originator=%s.", id, primaryOwner, origin);
      }
      if (origin.equals(rpcManager.getAddress())) {
         commandAckCollector.ack(id, origin);
      } else {
         //Collection<Address> recipients = origin.equals(primaryOwner) ?
         //      Collections.singletonList(primaryOwner) : Arrays.asList(primaryOwner, origin);
         rpcManager.invokeRemotelyAsync(Collections.singleton(origin), createAck(id), asyncRpcOptions);
      }
   }

   private BackupAckCommand createAck(CommandInvocationId id) {
      return commandsFactory.buildBackupAckCommand(id);
   }

   private Address getPrimaryOwner(Object key) {
      return distributionManager.getPrimaryLocation(key);
   }

   private KeyOwnership getKeyOwnership(Object key) {
      List<Address> owners = distributionManager.getConsistentHash().locateOwners(key);
      return KeyOwnership.ownership(owners, localAddress);
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
