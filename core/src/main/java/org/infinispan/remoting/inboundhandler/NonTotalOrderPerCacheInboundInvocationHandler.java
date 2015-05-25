package org.infinispan.remoting.inboundhandler;

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

import java.util.Collection;
import java.util.Collections;
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
               runnable = createLockAwareRunnable(command, (RemoteLockCommand) command, reply,
                                                  extractCommandTopologyId((SingleRpcCommand) command), true, onExecutorService);
               break;
            case MultipleRpcCommand.COMMAND_ID:
               runnable = createLockAwareRunnable(command, (RemoteLockCommand) command, reply,
                                                  extractCommandTopologyId((MultipleRpcCommand) command), true, onExecutorService);
               break;
            case StateRequestCommand.COMMAND_ID:
               // StateRequestCommand is special in that it doesn't need transaction data
               // In fact, waiting for transaction data could cause a deadlock
               runnable = createDefaultRunnable(command, reply,
                                                extractCommandTopologyId(((StateRequestCommand) command)), false, onExecutorService);
               break;
            case PrepareCommand.COMMAND_ID:
            case VersionedPrepareCommand.COMMAND_ID:
               runnable = createDefaultRunnable(command, reply, extractCommandTopologyId((PrepareCommand) command),
                                                true, onExecutorService);
               break;
            case LockControlCommand.COMMAND_ID:
               runnable = createDefaultRunnable(command, reply, extractCommandTopologyId((LockControlCommand) command),
                                                true, onExecutorService);
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

   protected final BlockingRunnable createLockAwareRunnable(CacheRpcCommand command, RemoteLockCommand remoteLockCommand, Reply reply,
                                                            int commandTopologyId, boolean waitTransactionalData,
                                                            boolean onExecutorService) {
      final TopologyMode topologyMode = TopologyMode.create(onExecutorService, waitTransactionalData);
      Collection<Object> keysToLock = getKeysToLock(remoteLockCommand);
      if (onExecutorService && !keysToLock.isEmpty()) {
         final long timeoutMillis = remoteLockCommand.hasZeroLockAcquisition() ? 0 : lockManager.getDefaultTimeoutMillis();
         final LockPromise lockPromise = lockManager.lockAll(keysToLock, remoteLockCommand.getLockOwner(), timeoutMillis, TimeUnit.MILLISECONDS);
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

   private Collection<Object> getKeysToLock(RemoteLockCommand command) {
      if (command.hasSkipLocking()) {
         return Collections.emptyList();
      }
      return command.getKeysToLock();
   }
}
