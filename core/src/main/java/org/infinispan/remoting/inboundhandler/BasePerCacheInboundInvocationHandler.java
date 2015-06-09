package org.infinispan.remoting.inboundhandler;

import org.infinispan.commands.CancellableCommand;
import org.infinispan.commands.CancellationService;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.remote.MultipleRpcCommand;
import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.ResponseGenerator;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.statetransfer.StateTransferLock;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.util.concurrent.BlockingRunnable;
import org.infinispan.util.concurrent.BlockingTaskAwareExecutorService;
import org.infinispan.util.logging.Log;

import static org.infinispan.factories.KnownComponentNames.REMOTE_COMMAND_EXECUTOR;

/**
 * Implementation with the default handling methods and utilities methods.
 *
 * @author Pedro Ruivo
 * @since 7.1
 */
public abstract class BasePerCacheInboundInvocationHandler implements PerCacheInboundInvocationHandler {
   protected static int NO_TOPOLOGY_COMMAND = Integer.MIN_VALUE;
   protected BlockingTaskAwareExecutorService remoteCommandsExecutor;
   protected StateTransferLock stateTransferLock;
   protected StateTransferManager stateTransferManager;
   private ResponseGenerator responseGenerator;
   private CancellationService cancellationService;

   protected static int extractCommandTopologyId(SingleRpcCommand command) {
      ReplicableCommand innerCmd = command.getCommand();
      if (innerCmd instanceof TopologyAffectedCommand) {
         return extractCommandTopologyId((TopologyAffectedCommand) innerCmd);
      }
      return NO_TOPOLOGY_COMMAND;
   }

   protected static int extractCommandTopologyId(MultipleRpcCommand command) {
      int commandTopologyId = NO_TOPOLOGY_COMMAND;
      for (ReplicableCommand innerCmd : command.getCommands()) {
         if (innerCmd instanceof TopologyAffectedCommand) {
            commandTopologyId = Math.max(extractCommandTopologyId((TopologyAffectedCommand) innerCmd), commandTopologyId);
         }
      }
      return commandTopologyId;
   }

   protected static int extractCommandTopologyId(TopologyAffectedCommand command) {
      return command.getTopologyId();
   }

   @Inject
   public void injectDependencies(@ComponentName(REMOTE_COMMAND_EXECUTOR) BlockingTaskAwareExecutorService remoteCommandsExecutor,
                                  ResponseGenerator responseGenerator,
                                  CancellationService cancellationService,
                                  StateTransferLock stateTransferLock,
                                  StateTransferManager stateTransferManager) {
      this.remoteCommandsExecutor = remoteCommandsExecutor;
      this.responseGenerator = responseGenerator;
      this.cancellationService = cancellationService;
      this.stateTransferLock = stateTransferLock;
      this.stateTransferManager = stateTransferManager;
   }

   final Response invokePerform(CacheRpcCommand cmd) throws Throwable {
      try {
         if (isTraceEnabled()) {
            getLog().tracef("Calling perform() on %s", cmd);
         }
         if (cmd instanceof CancellableCommand) {
            cancellationService.register(Thread.currentThread(), ((CancellableCommand) cmd).getUUID());
         }
         return responseGenerator.getResponse(cmd, cmd.perform(null));
      } finally {
         if (cmd instanceof CancellableCommand) {
            cancellationService.unregister(((CancellableCommand) cmd).getUUID());
         }
      }
   }

   final StateTransferLock getStateTransferLock() {
      return stateTransferLock;
   }

   final ExceptionResponse exceptionHandlingCommand(CacheRpcCommand command, Throwable throwable) {
      getLog().exceptionHandlingCommand(command, throwable);
      return new ExceptionResponse(new CacheException("Problems invoking command.", throwable));
   }

   final ExceptionResponse exceptionHandlingCommand(CacheRpcCommand command, Exception exception) {
      getLog().exceptionHandlingCommand(command, exception);
      return new ExceptionResponse(exception);
   }

   final ExceptionResponse outdatedTopology(OutdatedTopologyException exception) {
      getLog().outdatedTopology(exception);
      return new ExceptionResponse(exception);
   }

   final ExceptionResponse interruptedException(CacheRpcCommand command) {
      getLog().shutdownHandlingCommand(command);
      return new ExceptionResponse(new CacheException("Cache is shutting down"));
   }

   protected final void unexpectedDeliverMode(ReplicableCommand command, DeliverOrder deliverOrder) {
      throw new IllegalArgumentException(String.format("Unexpected deliver mode %s for command%s", deliverOrder, command));
   }

   protected final void handleRunnable(BlockingRunnable runnable, boolean onExecutorService) {
      if (onExecutorService) {
         remoteCommandsExecutor.execute(runnable);
      } else {
         runnable.run();
      }
   }

   protected final boolean isCommandSentBeforeFirstTopology(int commandTopologyId) {
      if (0 <= commandTopologyId && commandTopologyId < stateTransferManager.getFirstTopologyAsMember()) {
         if (isTraceEnabled()) {
            getLog().tracef("Ignoring command sent before the local node was a member " +
                                          "(command topology id is %d)", commandTopologyId);
         }
         return true;
      }
      return false;
   }

   protected final BlockingRunnable createDefaultRunnable(final CacheRpcCommand command, final Reply reply,
                                                          final int commandTopologyId, final boolean waitTransactionalData,
                                                          final boolean onExecutorService) {
      return new DefaultTopologyRunnable(this, command, reply, TopologyMode.create(onExecutorService, waitTransactionalData), commandTopologyId);
   }

   protected abstract Log getLog();

   protected abstract boolean isTraceEnabled();
}
