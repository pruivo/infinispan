package org.infinispan.remoting.inboundhandler;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.remote.MultipleRpcCommand;
import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.tx.VersionedCommitCommand;
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.commands.tx.totalorder.TotalOrderCommitCommand;
import org.infinispan.commands.tx.totalorder.TotalOrderNonVersionedPrepareCommand;
import org.infinispan.commands.tx.totalorder.TotalOrderRollbackCommand;
import org.infinispan.commands.tx.totalorder.TotalOrderVersionedCommitCommand;
import org.infinispan.commands.tx.totalorder.TotalOrderVersionedPrepareCommand;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.statetransfer.StateRequestCommand;
import org.infinispan.util.concurrent.BlockingRunnable;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.concurrent.locks.LockPromise;
import org.infinispan.util.concurrent.locks.LockUtil;
import org.infinispan.util.concurrent.locks.order.RemoteLockCommand;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A {@link org.infinispan.remoting.inboundhandler.PerCacheInboundInvocationHandler} implementation for non-total order
 * caches.
 *
 * @author Pedro Ruivo
 * @since 7.1
 */
public class NonTotalOrderPerCacheInboundInvocationHandler extends BasePerCacheInboundInvocationHandler implements LockPromise.Listener {

   private static final Log log = LogFactory.getLog(NonTotalOrderPerCacheInboundInvocationHandler.class);
   private static final boolean trace = log.isTraceEnabled();

   private LockManager lockManager;
   private ClusteringDependentLogic clusteringDependentLogic;

   @Inject
   public void inject(LockManager lockManager, ClusteringDependentLogic clusteringDependentLogic) {
      this.lockManager = lockManager;
      this.clusteringDependentLogic = clusteringDependentLogic;
   }

   @Override
   public void handle(CacheRpcCommand command, Reply reply, DeliverOrder order) {
      if (order == DeliverOrder.TOTAL) {
         unexpectedDeliverMode(command, order);
      }
      try {
         boolean onExecutorService = !order.preserveOrder() && command.canBlock();
         BlockingRunnable runnable;
         int commandTopologyId;

         switch (command.getCommandId()) {
            case SingleRpcCommand.COMMAND_ID:
               commandTopologyId = extractCommandTopologyId((SingleRpcCommand) command);
               if (isCommandSentBeforeFirstTopology(commandTopologyId)) {
                  reply.reply(CacheNotFoundResponse.INSTANCE);
                  return;
               }
               runnable = createLockAwareRunnable(command, reply, commandTopologyId, true, onExecutorService,
                                                  getLockPromise((SingleRpcCommand) command));
               break;
            case MultipleRpcCommand.COMMAND_ID:
               commandTopologyId = extractCommandTopologyId((MultipleRpcCommand) command);
               if (isCommandSentBeforeFirstTopology(commandTopologyId)) {
                  reply.reply(CacheNotFoundResponse.INSTANCE);
                  return;
               }
               runnable = createLockAwareRunnable(command, reply, commandTopologyId, true, onExecutorService,
                                                  getLockPromise((MultipleRpcCommand) command));
               break;
            case StateRequestCommand.COMMAND_ID:
               // StateRequestCommand is special in that it doesn't need transaction data
               // In fact, waiting for transaction data could cause a deadlock
               commandTopologyId = extractCommandTopologyId((StateRequestCommand) command);
               if (isCommandSentBeforeFirstTopology(commandTopologyId)) {
                  reply.reply(CacheNotFoundResponse.INSTANCE);
                  return;
               }
               runnable = createDefaultRunnable(command, reply, commandTopologyId, false, onExecutorService);
               break;
            case PrepareCommand.COMMAND_ID:
            case VersionedPrepareCommand.COMMAND_ID:
            case TotalOrderNonVersionedPrepareCommand.COMMAND_ID:
            case TotalOrderVersionedPrepareCommand.COMMAND_ID:
            case CommitCommand.COMMAND_ID:
            case VersionedCommitCommand.COMMAND_ID:
            case TotalOrderCommitCommand.COMMAND_ID:
            case TotalOrderVersionedCommitCommand.COMMAND_ID:
            case RollbackCommand.COMMAND_ID:
            case TotalOrderRollbackCommand.COMMAND_ID:
            case LockControlCommand.COMMAND_ID:
               throw new IllegalArgumentException("Illegal Command ID: " + command.getCommandId());
            default:
               commandTopologyId = NO_TOPOLOGY_COMMAND;
               if (command instanceof TopologyAffectedCommand) {
                  commandTopologyId = extractCommandTopologyId((TopologyAffectedCommand) command);
               }
               if (isCommandSentBeforeFirstTopology(commandTopologyId)) {
                  reply.reply(CacheNotFoundResponse.INSTANCE);
                  return;
               }
               runnable = createDefaultRunnable(command, reply, commandTopologyId, true, onExecutorService);
               break;
         }
         handleRunnable(runnable, onExecutorService);
      } catch (Throwable throwable) {
         reply.reply(exceptionHandlingCommand(command, throwable));
      }
   }

   @Override
   public void onEvent(boolean acquired) {
      remoteCommandsExecutor.checkForReadyTasks();
   }

   @Override
   protected Log getLog() {
      return log;
   }

   @Override
   protected boolean isTraceEnabled() {
      return trace;
   }

   protected final BlockingRunnable createLockAwareRunnable(CacheRpcCommand command, Reply reply, int commandTopologyId,
                                                            boolean waitTransactionalData, boolean onExecutorService,
                                                            LockPromise lockPromise) {
      final TopologyMode topologyMode = TopologyMode.create(onExecutorService, waitTransactionalData);
      if (onExecutorService) {
         lockPromise.addListener(this);
         return new DefaultTopologyRunnable(this, command, reply, topologyMode, commandTopologyId) {
            @Override
            public boolean isReady() {
               return super.isReady() && lockPromise.isAvailable();
            }
         };
      } else {
         return new DefaultTopologyRunnable(this, command, reply, topologyMode, commandTopologyId);
      }
   }

   private LockPromise getLockPromise(RemoteLockCommand command) {
      if (command.hasSkipLocking()) {
         return LockPromise.NO_OP;
      }
      Collection<Object> keys = command.getKeysToLock();
      if (keys.isEmpty()) {
         return LockPromise.NO_OP;
      }
      Collection<Object> keysToLock = new ArrayList<>(keys.size());
      LockUtil.filterByLockOwnership(keys, keysToLock, null, clusteringDependentLogic);
      final long timeoutMillis = command.hasZeroLockAcquisition() ? 0 : lockManager.getDefaultTimeoutMillis();
      return lockManager.lockAll(keysToLock, command.getLockOwner(), timeoutMillis, TimeUnit.MILLISECONDS);
   }

   private LockPromise getLockPromise(SingleRpcCommand singleRpcCommand) {
      ReplicableCommand command = singleRpcCommand.getCommand();
      return command instanceof RemoteLockCommand ? getLockPromise((RemoteLockCommand) command) : LockPromise.NO_OP;
   }

   private LockPromise getLockPromise(MultipleRpcCommand multipleRpcCommand) {
      ReplicableCommand[] commands = multipleRpcCommand.getCommands();
      List<LockPromise> lockPromiseList = new ArrayList<>(commands.length);
      for (ReplicableCommand command : commands) {
         if (command instanceof RemoteLockCommand) {
            lockPromiseList.add(getLockPromise((RemoteLockCommand) command));
         }
      }
      if (lockPromiseList.isEmpty()) {
         return LockPromise.NO_OP;
      } else if (lockPromiseList.size() == 1) {
         return lockPromiseList.get(0);
      }
      CompositeLockPromise promise = new CompositeLockPromise(lockPromiseList);
      promise.registerListener();
      return promise;
   }

   /**
    * Only used to check when all the {@link LockPromise} are available.
    */
   private static class CompositeLockPromise implements LockPromise, LockPromise.Listener {

      private final List<LockPromise> lockPromiseList;
      private final AtomicBoolean notify;
      private volatile Listener checkReadyTasks;

      private CompositeLockPromise(List<LockPromise> lockPromiseList) {
         this.lockPromiseList = lockPromiseList;
         notify = new AtomicBoolean(false);
      }

      public void registerListener() {
         for (LockPromise lockPromise : lockPromiseList) {
            lockPromise.addListener(this);
         }
      }

      @Override
      public boolean isAvailable() {
         for (LockPromise lockPromise : lockPromiseList) {
            if (!lockPromise.isAvailable()) {
               return false;
            }
         }
         return true;
      }

      @Override
      public void lock() throws InterruptedException, TimeoutException {/*no-op*/}

      @Override
      public void addListener(Listener listener) {
         this.checkReadyTasks = listener;
         onEvent(false); //check if available
      }

      @Override
      public void onEvent(boolean acquired) {
         Listener listener = checkReadyTasks;
         if (isAvailable() && listener != null && notify.compareAndSet(false, true)) {
            listener.onEvent(acquired); //it doesn't matter the acquired value.
         }
      }
   }
}
