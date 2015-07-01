package org.infinispan.remoting.inboundhandler;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.remote.MultipleRpcCommand;
import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.statetransfer.StateRequestCommand;
import org.infinispan.util.concurrent.BlockingRunnable;
import org.infinispan.util.concurrent.locks.LockManagerV8;
import org.infinispan.util.concurrent.locks.LockPromise;
import org.infinispan.util.concurrent.locks.order.RemoteLockCommand;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A {@link org.infinispan.remoting.inboundhandler.PerCacheInboundInvocationHandler} implementation for non-total order
 * caches.
 *
 * @author Pedro Ruivo
 * @since 7.1
 */
public class NonTotalOrderPerCacheInboundInvocationHandler extends BasePerCacheInboundInvocationHandler {

   private static final Log log = LogFactory.getLog(NonTotalOrderPerCacheInboundInvocationHandler.class);
   private static final boolean trace = log.isTraceEnabled();

   private LockManagerV8 lockManager;

   @Inject
   public void inject(LockManagerV8 lockManager) {
      this.lockManager = lockManager;
   }

   @Override
   public void handle(CacheRpcCommand command, Reply reply, DeliverOrder order) {
      if (order == DeliverOrder.TOTAL) {
         unexpectedDeliverMode(command, order);
      }
      try {
         boolean onExecutorService = !order.preserveOrder() && command.canBlock();
         BlockingRunnable runnable;

         switch (command.getCommandId()) {
            case SingleRpcCommand.COMMAND_ID:
               runnable = createLockAwareRunnable(command, reply, extractCommandTopologyId((SingleRpcCommand) command),
                                                  true, onExecutorService, getLockPromise((SingleRpcCommand) command));
               break;
            case MultipleRpcCommand.COMMAND_ID:
               runnable = createLockAwareRunnable(command, reply, extractCommandTopologyId((MultipleRpcCommand) command),
                                                  true, onExecutorService, getLockPromise((MultipleRpcCommand) command));
               break;
            case StateRequestCommand.COMMAND_ID:
               // StateRequestCommand is special in that it doesn't need transaction data
               // In fact, waiting for transaction data could cause a deadlock
               runnable = createDefaultRunnable(command, reply, extractCommandTopologyId(((StateRequestCommand) command)),
                                                false, onExecutorService);
               break;
            case PrepareCommand.COMMAND_ID:
            case VersionedPrepareCommand.COMMAND_ID:
               runnable = createLockAwareRunnable(command, reply, extractCommandTopologyId((PrepareCommand) command),
                                                  true, onExecutorService, getLockPromise((PrepareCommand) command));
               break;
            case LockControlCommand.COMMAND_ID:
               runnable = createLockAwareRunnable(command, reply, extractCommandTopologyId((LockControlCommand) command),
                                                  true, onExecutorService, getLockPromise((LockControlCommand) command));
               break;
            default:
               int commandTopologyId = NO_TOPOLOGY_COMMAND;
               if (command instanceof TopologyAffectedCommand) {
                  commandTopologyId = extractCommandTopologyId((TopologyAffectedCommand) command);
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
      //TODO filter primary owner only!
      final long timeoutMillis = command.hasZeroLockAcquisition() ? 0 : lockManager.getDefaultTimeoutMillis();
      return lockManager.lockAll(keys, command.getLockOwner(), timeoutMillis, TimeUnit.MILLISECONDS);
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
      return lockPromiseList.isEmpty() ? LockPromise.NO_OP : null; //TODO collection!
   }


}
