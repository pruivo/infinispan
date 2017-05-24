package org.infinispan.remoting.inboundhandler;

import static org.infinispan.remoting.inboundhandler.DeliverOrder.NONE;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;

import org.infinispan.IllegalLifecycleStateException;
import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.write.BackupAckCommand;
import org.infinispan.commands.write.BackupMultiKeyAckCommand;
import org.infinispan.commands.write.BackupPutMapRpcCommand;
import org.infinispan.commands.write.BackupWriteRpcCommand;
import org.infinispan.commands.write.ExceptionAckCommand;
import org.infinispan.distribution.TriangleOrderManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.remoting.inboundhandler.action.Action;
import org.infinispan.remoting.inboundhandler.action.ActionState;
import org.infinispan.remoting.inboundhandler.action.ActionStatus;
import org.infinispan.remoting.inboundhandler.action.DefaultReadyAction;
import org.infinispan.remoting.inboundhandler.action.ReadyAction;
import org.infinispan.remoting.inboundhandler.action.TriangleOrderAction;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.statetransfer.StateRequestCommand;
import org.infinispan.util.concurrent.BlockingRunnable;
import org.infinispan.util.concurrent.BlockingTaskAwareExecutorService;
import org.infinispan.util.concurrent.CommandAckCollector;
import org.infinispan.util.concurrent.locks.LockListener;
import org.infinispan.util.concurrent.locks.LockState;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * A {@link PerCacheInboundInvocationHandler} implementation for non-transactional and distributed caches that uses the
 * triangle algorithm.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class TrianglePerCacheInboundInvocationHandler extends BasePerCacheInboundInvocationHandler implements
      LockListener, Action {

   private static final Log log = LogFactory.getLog(TrianglePerCacheInboundInvocationHandler.class);
   private static final boolean trace = log.isTraceEnabled();

   private ClusteringDependentLogic clusteringDependentLogic;
   private TriangleOrderManager triangleOrderManager;
   private RpcManager rpcManager;
   private CommandAckCollector commandAckCollector;
   private CommandsFactory commandsFactory;
   private Address localAddress;

   @Inject
   public void inject(ClusteringDependentLogic clusteringDependentLogic,
         TriangleOrderManager anotherTriangleOrderManager, RpcManager rpcManager,
         CommandAckCollector commandAckCollector, CommandsFactory commandsFactory) {
      this.clusteringDependentLogic = clusteringDependentLogic;
      this.triangleOrderManager = anotherTriangleOrderManager;
      this.rpcManager = rpcManager;
      this.commandAckCollector = commandAckCollector;
      this.commandsFactory = commandsFactory;
   }

   @Start
   public void start() {
      localAddress = rpcManager.getAddress();
   }

   @Override
   public void handle(CacheRpcCommand command, Reply reply, DeliverOrder order) {
      if (order == DeliverOrder.TOTAL) {
         unexpectedDeliverMode(command, order);
      }
      try {
         switch (command.getCommandId()) {
            case BackupWriteRpcCommand.COMMAND_ID:
               handleBackupWriteRpcCommand((BackupWriteRpcCommand) command);
               return;
            case BackupPutMapRpcCommand.COMMAND_ID:
               handleBackupPutMapRpcCommand((BackupPutMapRpcCommand) command);
               return;
            case BackupAckCommand.COMMAND_ID:
               handleBackupAckCommand((BackupAckCommand) command);
               return;
            case BackupMultiKeyAckCommand.COMMAND_ID:
               handleBackupMultiKeyAckCommand((BackupMultiKeyAckCommand) command);
               return;
            case ExceptionAckCommand.COMMAND_ID:
               handleExceptionAck((ExceptionAckCommand) command);
               return;
            case StateRequestCommand.COMMAND_ID:
               handleStateRequestCommand((StateRequestCommand) command, reply, order);
               return;
            default:
               handleDefaultCommand(command, reply, order);
         }
      } catch (Throwable throwable) {
         reply.reply(exceptionHandlingCommand(command, throwable));
      }
   }

   //lock listener interface
   @Override
   public void onEvent(LockState state) {
      remoteCommandsExecutor.checkForReadyTasks();
   }

   //action interface
   @Override
   public ActionStatus check(ActionState state) {
      return isCommandSentBeforeFirstTopology(state.getCommandTopologyId()) ?
            ActionStatus.CANCELED :
            ActionStatus.READY;
   }

   public TriangleOrderManager getTriangleOrderManager() {
      return triangleOrderManager;
   }

   public BlockingTaskAwareExecutorService getRemoteExecutor() {
      return remoteCommandsExecutor;
   }

   public ClusteringDependentLogic getClusteringDependentLogic() {
      return clusteringDependentLogic;
   }

   @Override
   public void onFinally(ActionState state) {
      //no-op
      //needed for ConditionalOperationPrimaryOwnerFailTest
      //it mocks this class and when Action.onFinally is invoked, it doesn't behave well with the default implementation
      //in the interface.
   }

   @Override
   protected Log getLog() {
      return log;
   }

   @Override
   protected boolean isTraceEnabled() {
      return trace;
   }

   private void handleStateRequestCommand(StateRequestCommand command, Reply reply, DeliverOrder order) {
      final int commandTopologyId = extractCommandTopologyId(command);
      CompletableFuture<Void> future = getStateTransferLock().topologyFuture(Math.max(commandTopologyId, 0))
            .thenApply(new CheckTopologyAndInvoke(commandTopologyId, command, reply));

      if (order.preserveOrder()) {
         future.join();
      }
   }

   private void handleDefaultCommand(CacheRpcCommand command, Reply reply, DeliverOrder order) {
      final int commandTopologyId = extractCommandTopologyId(command);
      CompletableFuture<Void> future = getStateTransferLock().transactionDataFuture(Math.max(commandTopologyId, 0))
            .thenApply(new CheckTopologyAndInvoke(commandTopologyId, command, reply));

      if (order.preserveOrder()) {
         future.join();
      }
   }

   private void handleBackupPutMapRpcCommand(BackupPutMapRpcCommand command) {
      final int topologyId = command.getTopologyId();
      ReadyAction readyAction = createTriangleOrderAction(command, topologyId, command.getSequence(),
            command.getMap().keySet().iterator().next());
      BlockingRunnable runnable = createBackupPutMapRunnable(command, topologyId, readyAction);
      remoteCommandsExecutor.execute(runnable);
   }

   private void handleBackupWriteRpcCommand(BackupWriteRpcCommand command) {
      final int commandTopologyId = command.getTopologyId();
      getStateTransferLock().transactionDataFuture(Math.max(commandTopologyId, 0))
            .thenApply(new CheckTopologyAndBackupWriteInvoke(commandTopologyId, command)).join();
   }

   private void handleExceptionAck(ExceptionAckCommand command) {
      command.ack();
   }

   private void handleBackupMultiKeyAckCommand(BackupMultiKeyAckCommand command) {
      command.ack();
   }

   private void handleBackupAckCommand(BackupAckCommand command) {
      command.ack();
   }

   private void replyException(Throwable t, CacheRpcCommand command, Reply reply) {
      Throwable throwable = unwrap(t);
      if (throwable instanceof InterruptedException) {
         reply.reply(interruptedException(command));
      } else if (throwable instanceof OutdatedTopologyException) {
         reply.reply(outdatedTopology((OutdatedTopologyException) throwable));
      } else if (throwable instanceof IllegalLifecycleStateException) {
         reply.reply(CacheNotFoundResponse.INSTANCE);
      } else {
         reply.reply(exceptionHandlingCommand(command, throwable));
      }
   }

   private Throwable unwrap(Throwable throwable) {
      if (throwable instanceof CompletionException && throwable.getCause() != null) {
         throwable = throwable.getCause();
      }
      return throwable;
   }

   private void sendExceptionAck(CommandInvocationId id, Throwable throwable, int topologyId) {
      final Address origin = id.getAddress();
      if (trace) {
         log.tracef("Sending exception ack for command %s. Originator=%s.", id, origin);
      }
      if (origin.equals(localAddress)) {
         commandAckCollector.completeExceptionally(id.getId(), throwable, topologyId);
      } else {
         rpcManager.sendTo(origin, commandsFactory.buildExceptionAckCommand(id.getId(), throwable, topologyId), NONE);
      }
   }

   private void sendBackupAck(CommandInvocationId id, int topologyId) {
      final Address origin = id.getAddress();
      boolean isLocal = localAddress.equals(origin);
      if (trace) {
         log.tracef("Sending ack for command %s. isLocal? %s.", id, isLocal);
      }
      if (isLocal) {
         commandAckCollector.backupAck(id.getId(), origin, topologyId);
      } else {
         rpcManager.sendTo(origin, commandsFactory.buildBackupAckCommand(id.getId(), topologyId), NONE);
      }
   }

   private BlockingRunnable createBackupWriteRpcRunnable(BackupWriteRpcCommand command, int commandTopologyId,
         ReadyAction readyAction) {
      readyAction.addListener(remoteCommandsExecutor::checkForReadyTasks);
      return new DefaultTopologyRunnable(this, command, Reply.NO_OP, TopologyMode.READY_TX_DATA, commandTopologyId,
            false) {
         @Override
         public boolean isReady() {
            return super.isReady() && readyAction.isReady();
         }

         @Override
         protected void onException(Throwable throwable) {
            super.onException(throwable);
            readyAction.onException();
            readyAction.onFinally(); //notified TriangleOrderManager before sending the ack.
            sendExceptionAck(((BackupWriteRpcCommand) command).getCommandInvocationId(), throwable, commandTopologyId);
         }

         @Override
         protected void afterInvoke() {
            super.afterInvoke();
            readyAction.onFinally();
            sendBackupAck(((BackupWriteRpcCommand) command).getCommandInvocationId(), commandTopologyId);
         }
      };
   }

   private void sendPutMapBackupAck(CommandInvocationId id, int topologyId, int segment) {
      final Address origin = id.getAddress();
      if (trace) {
         log.tracef("Sending ack for command %s. Originator=%s.", id, origin);
      }
      if (id.getAddress().equals(localAddress)) {
         commandAckCollector.multiKeyBackupAck(id.getId(), localAddress, segment, topologyId);
      } else {
         rpcManager
               .sendTo(origin, commandsFactory.buildBackupMultiKeyAckCommand(id.getId(), segment, topologyId), NONE);
      }
   }

   private BlockingRunnable createBackupPutMapRunnable(BackupPutMapRpcCommand command, int commandTopologyId,
         ReadyAction readyAction) {
      readyAction.addListener(remoteCommandsExecutor::checkForReadyTasks);
      return new DefaultTopologyRunnable(this, command, Reply.NO_OP, TopologyMode.READY_TX_DATA, commandTopologyId,
            false) {
         @Override
         public boolean isReady() {
            return super.isReady() && readyAction.isReady();
         }

         @Override
         protected void onException(Throwable throwable) {
            super.onException(throwable);
            readyAction.onException();
            readyAction.onFinally();
            sendExceptionAck(((BackupPutMapRpcCommand) command).getCommandInvocationId(), throwable, commandTopologyId);
         }

         @Override
         protected void afterInvoke() {
            super.afterInvoke();
            readyAction.onFinally();
            Object key = ((BackupPutMapRpcCommand) command).getMap().keySet().iterator().next();
            int segment = clusteringDependentLogic.getCacheTopology().getDistribution(key).segmentId();
            sendPutMapBackupAck(((BackupPutMapRpcCommand) command).getCommandInvocationId(), commandTopologyId,
                  segment);
         }
      };
   }

   private ReadyAction createTriangleOrderAction(ReplicableCommand command, int topologyId, long sequence, Object key) {
      return new DefaultReadyAction(new ActionState(command, topologyId, 0), this,
            new TriangleOrderAction(this, sequence, key));
   }

   private class CheckTopologyAndInvoke implements Function<Void, Void> {

      private final int commandTopologyId;
      private final CacheRpcCommand command;
      private final Reply reply;

      private CheckTopologyAndInvoke(int commandTopologyId, CacheRpcCommand command, Reply reply) {
         this.commandTopologyId = commandTopologyId;
         this.command = command;
         this.reply = reply;
      }

      @Override
      public Void apply(Void nil) {
         if (isCommandSentBeforeFirstTopology(commandTopologyId)) {
            reply.reply(CacheNotFoundResponse.INSTANCE);
         } else {
            try {
               invokeCommand(command).whenComplete((rsp, throwable) -> {
                  if (throwable != null) {
                     replyException(throwable, command, reply);
                  } else {
                     reply.reply(rsp);
                  }
               });
            } catch (Throwable throwable) {
               reply.reply(exceptionHandlingCommand(command, throwable));
            }
         }
         return null;
      }
   }

   private class CheckTopologyAndBackupWriteInvoke implements Function<Void, Void> {

      private final int commandTopologyId;
      private final BackupWriteRpcCommand command;

      private CheckTopologyAndBackupWriteInvoke(int commandTopologyId, BackupWriteRpcCommand command) {
         this.commandTopologyId = commandTopologyId;
         this.command = command;
      }

      @Override
      public Void apply(Void nil) {
         if (isCommandSentBeforeFirstTopology(commandTopologyId)) {
            return null;
         } else {
            try {
               invokeCommand(command).whenComplete((rsp, throwable) -> {
                  if (throwable != null) {
                     sendExceptionAck(command.getCommandInvocationId(), throwable, commandTopologyId);
                  } else {
                     sendBackupAck(command.getCommandInvocationId(), commandTopologyId);
                  }
               });
            } catch (Throwable throwable) {
               sendExceptionAck(command.getCommandInvocationId(), throwable, commandTopologyId);
            }
         }
         return null;
      }
   }
}
