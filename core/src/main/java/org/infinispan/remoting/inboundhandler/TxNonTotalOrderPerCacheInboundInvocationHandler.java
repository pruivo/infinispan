package org.infinispan.remoting.inboundhandler;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.remote.MultipleRpcCommand;
import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.commons.util.Notifier;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.context.impl.RemoteTxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.statetransfer.StateRequestCommand;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.concurrent.BlockingRunnable;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.concurrent.locks.LockPromise;
import org.infinispan.util.concurrent.locks.LockUtil;
import org.infinispan.util.concurrent.locks.PendingLockManager;
import org.infinispan.util.concurrent.locks.impl.PendingLockPromise;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * A {@link PerCacheInboundInvocationHandler} implementation for non-total order caches.
 *
 * @author Pedro Ruivo
 * @since 7.1
 */
public class TxNonTotalOrderPerCacheInboundInvocationHandler extends BasePerCacheInboundInvocationHandler {

   private static final Log log = LogFactory.getLog(TxNonTotalOrderPerCacheInboundInvocationHandler.class);
   private static final boolean trace = log.isTraceEnabled();

   private LockManager lockManager;
   private ClusteringDependentLogic clusteringDependentLogic;
   private PendingLockManager pendingLockManager;
   private boolean pessimisticLocking;

   @Inject
   public void inject(LockManager lockManager, ClusteringDependentLogic clusteringDependentLogic, Configuration configuration,
                      PendingLockManager pendingLockManager) {
      this.lockManager = lockManager;
      this.clusteringDependentLogic = clusteringDependentLogic;
      this.pendingLockManager = pendingLockManager;
      this.pessimisticLocking = configuration.transaction().lockingMode() == LockingMode.PESSIMISTIC;
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
               runnable = createDefaultRunnable(command, reply, commandTopologyId, true, onExecutorService);
               break;
            case MultipleRpcCommand.COMMAND_ID:
               commandTopologyId = extractCommandTopologyId((MultipleRpcCommand) command);
               if (isCommandSentBeforeFirstTopology(commandTopologyId)) {
                  reply.reply(CacheNotFoundResponse.INSTANCE);
                  return;
               }
               runnable = createReadyActionAwareRunnable(command, reply, commandTopologyId, true, onExecutorService,
                                                         createReadyActionForMultipleRpcCommand((MultipleRpcCommand) command));
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
               commandTopologyId = extractCommandTopologyId((PrepareCommand) command);
               if (isCommandSentBeforeFirstTopology(commandTopologyId)) {
                  reply.reply(CacheNotFoundResponse.INSTANCE);
                  return;
               }
               if (pessimisticLocking) {
                  runnable = createDefaultRunnable(command, reply, commandTopologyId, true, onExecutorService);
               } else {
                  runnable = createReadyActionAwareRunnable(command, reply, commandTopologyId, true, onExecutorService,
                                                            createReadyActionForPrepareCommand((PrepareCommand) command));
               }
               break;
            case LockControlCommand.COMMAND_ID:
               commandTopologyId = extractCommandTopologyId((LockControlCommand) command);
               if (isCommandSentBeforeFirstTopology(commandTopologyId)) {
                  reply.reply(CacheNotFoundResponse.INSTANCE);
                  return;
               }
               runnable = createReadyActionAwareRunnable(command, reply, commandTopologyId, true, onExecutorService,
                                                         createReadyActionForLockControlCommand((LockControlCommand) command));
               break;
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

   private ReadyAction createReadyActionForLockControlCommand(LockControlCommand command) {
      if (command.hasSkipLocking()) {
         return null;
      }
      Collection<Object> keys = command.getKeysToLock();
      if (keys.isEmpty()) {
         return null;
      }
      List<Object> keysToLock = new ArrayList<>(keys.size());
      LockUtil.filterByLockOwnership(keys, keysToLock, null, clusteringDependentLogic);
      if (keysToLock.isEmpty()) {
         return null;
      }
      final long timeoutMillis = command.hasZeroLockAcquisition() ? 0 : lockManager.getDefaultTimeoutMillis();
      final RemoteTxInvocationContext context = command.createContext();
      if (keysToLock.size() == 1) {
         return new SingleKeyReadyAction(context, timeoutMillis, keysToLock.get(0));
      }
      return new MultipleKeysReadyAction(context, timeoutMillis, keysToLock);
   }

   private ReadyAction createReadyActionForPrepareCommand(PrepareCommand command) {
      if (command.hasSkipLocking()) {
         return null;
      }
      Collection<Object> keys = command.getKeysToLock();
      if (keys.isEmpty()) {
         return null;
      }
      List<Object> keysToLock = new ArrayList<>(keys.size());
      LockUtil.filterByLockOwnership(keys, keysToLock, null, clusteringDependentLogic);
      if (keysToLock.isEmpty()) {
         return null;
      }
      final long timeoutMillis = command.hasZeroLockAcquisition() ? 0 : lockManager.getDefaultTimeoutMillis();
      final RemoteTxInvocationContext context = command.createContext();
      if (context == null) {
         return null;
      }
      if (keysToLock.size() == 1) {
         return new SingleKeyReadyAction(context, timeoutMillis, keysToLock.get(0));
      }
      return new MultipleKeysReadyAction(context, timeoutMillis, keysToLock);
   }

   private ReadyAction createReadyActionForMultipleRpcCommand(MultipleRpcCommand command) {
      ReplicableCommand[] commands = command.getCommands();
      List<ReadyAction> list = new ArrayList<>(commands.length);
      for (ReplicableCommand cmd : commands) {
         if (cmd instanceof LockControlCommand) {
            ReadyAction action = createReadyActionForLockControlCommand((LockControlCommand) cmd);
            if (action != null) {
               list.add(action);
            }
         } else if (!pessimisticLocking && cmd instanceof PrepareCommand) {
            ReadyAction action = createReadyActionForPrepareCommand((PrepareCommand) cmd);
            if (action != null) {
               list.add(action);
            }
         }
      }
      if (list.isEmpty()) {
         return null;
      } else if (list.size() == 1) {
         return list.get(0);
      }

      return new CompositeReadyAction(list);
   }

   @Override
   protected Log getLog() {
      return log;
   }

   @Override
   protected boolean isTraceEnabled() {
      return trace;
   }

   protected final BlockingRunnable createReadyActionAwareRunnable(CacheRpcCommand command, Reply reply, int commandTopologyId,
                                                                   boolean waitTransactionalData, boolean onExecutorService,
                                                                   ReadyAction readyAction) {
      if (readyAction == null) {
         return createDefaultRunnable(command, reply, commandTopologyId, waitTransactionalData, onExecutorService);
      }
      final TopologyMode topologyMode = TopologyMode.create(onExecutorService, waitTransactionalData);
      if (onExecutorService) {
         readyAction.init();
         readyAction.addListener(remoteCommandsExecutor::checkForReadyTasks);
         return new DefaultTopologyRunnable(this, command, reply, topologyMode, commandTopologyId) {
            @Override
            public boolean isReady() {
               return super.isReady() && readyAction.isReady();
            }
         };
      } else {
         return new DefaultTopologyRunnable(this, command, reply, topologyMode, commandTopologyId);
      }
   }

   private class CompositeReadyAction implements ReadyAction, ReadyActionListener {

      private final Collection<ReadyAction> actions;
      private final AtomicBoolean notify;
      private volatile ReadyActionListener listener;

      private CompositeReadyAction(Collection<ReadyAction> actions) {
         this.actions = actions;
         notify = new AtomicBoolean(false);
      }

      @Override
      public void init() {
         for (ReadyAction action : actions) {
            action.init();
            action.addListener(this);
         }
      }

      @Override
      public boolean isReady() {
         for (ReadyAction action : actions) {
            if (!action.isReady()) {
               return false;
            }
         }
         return true;
      }

      @Override
      public void addListener(ReadyActionListener listener) {
         this.listener = listener;
      }

      @Override
      public void onReady() {
         ReadyActionListener actionListener = listener;
         if (isReady() && actionListener != null && notify.compareAndSet(false, true)) {
            actionListener.onReady();
         }
      }
   }

   private class SingleKeyReadyAction extends BaseReadyAction {

      private final Object key;

      public SingleKeyReadyAction(RemoteTxInvocationContext context, long timeout, Object key) {
         super(context, timeout);
         this.key = key;
      }

      @Override
      protected PendingLockPromise checkPendingLock(RemoteTxInvocationContext context, long timeout) {
         return pendingLockManager.checkPendingForKey(context, key, timeout, TimeUnit.MILLISECONDS);
      }

      @Override
      protected LockPromise acquireLock(RemoteTxInvocationContext context, long timeout) {
         return lockManager.lock(key, context.getLockOwner(), timeout, TimeUnit.MILLISECONDS);
      }
   }

   private class MultipleKeysReadyAction extends BaseReadyAction {

      private final Collection<Object> keys;

      public MultipleKeysReadyAction(RemoteTxInvocationContext context, long timeout, Collection<Object> keys) {
         super(context, timeout);
         this.keys = keys;
      }

      @Override
      protected PendingLockPromise checkPendingLock(RemoteTxInvocationContext context, long timeout) {
         return pendingLockManager.checkPendingForAllKeys(context, keys, timeout, TimeUnit.MILLISECONDS);
      }

      @Override
      protected LockPromise acquireLock(RemoteTxInvocationContext context, long timeout) {
         return lockManager.lockAll(keys, context.getLockOwner(), timeout, TimeUnit.MILLISECONDS);
      }
   }

   private abstract class BaseReadyAction implements ReadyAction, PendingLockPromise.Listener, LockPromise.Listener, Notifier.Invoker<ReadyActionListener> {
      private final RemoteTxInvocationContext context;
      private final long timeout; //millis
      private final AtomicReferenceFieldUpdater<BaseReadyAction, ReadyActionStatus> statusFieldUpdater;
      private final Notifier<ReadyActionListener> notifier;

      private volatile ReadyActionStatus status;
      private volatile PendingLockPromise pendingLockPromise;
      private volatile LockPromise lockPromise;

      public BaseReadyAction(RemoteTxInvocationContext context, long timeout) {
         this.context = context;
         this.timeout = timeout;
         this.statusFieldUpdater = AtomicReferenceFieldUpdater.newUpdater(BaseReadyAction.class, ReadyActionStatus.class, "status");
         this.notifier = new Notifier<>(this);
      }

      @Override
      public final void init() {
         status = ReadyActionStatus.PENDING_TX;
         PendingLockPromise promise = checkPendingLock(context, timeout); //allow JVM to cache it
         pendingLockPromise = promise; //make it visible before add the listener.
         promise.addListener(this);
      }

      @Override
      public final boolean isReady() {
         //note: needs to be thread safe since it can be invoked multiple times by different threads.
         boolean isReady = false;
         switch (status) {
            case PENDING_TX:
               final PendingLockPromise promise = pendingLockPromise; //allow JVM to cache it
               if (promise.isReady()) {
                  if (promise.hasTimedOut() && cas(ReadyActionStatus.PENDING_TX, ReadyActionStatus.PENDING_TX_TIMEOUT)) {
                     isReady = true;
                  } else if (cas(ReadyActionStatus.PENDING_TX, ReadyActionStatus.LOCKS)) {
                     final long remaining = promise == PendingLockPromise.NO_OP ? timeout : promise.getRemainingTimeout();
                     LockPromise newPromise = acquireLock(context, remaining);
                     lockPromise = newPromise; //make it visible before add the listener.
                     newPromise.addListener(this);
                     isReady = newPromise.isAvailable();
                  }
               }
               break;
            case PENDING_TX_TIMEOUT:
               isReady = true;
               break;
            case LOCKS:
               final LockPromise otherPromise = lockPromise; //allow JVM to cache it
               isReady = otherPromise != null && otherPromise.isAvailable();
               break;
         }
         if (isReady) {
            notifier.fireListener(); //multiple invocations are not a problem.
         }
         return isReady;
      }

      @Override
      public final void addListener(ReadyActionListener listener) {
         notifier.add(listener);
      }

      @Override
      public void onEvent(boolean acquired) {
         isReady();
      }

      @Override
      public void onReady() {
         isReady();
      }

      private boolean cas(ReadyActionStatus expectedStatus, ReadyActionStatus newStatus) {
         return statusFieldUpdater.compareAndSet(this, expectedStatus, newStatus);
      }

      protected abstract PendingLockPromise checkPendingLock(RemoteTxInvocationContext context, long timeout);

      protected abstract LockPromise acquireLock(RemoteTxInvocationContext context, long timeout);

      @Override
      public void invoke(ReadyActionListener invoker) {
         invoker.onReady();
      }
   }

   private interface ReadyAction {
      void init();

      boolean isReady();

      void addListener(ReadyActionListener listener);
   }

   private interface ReadyActionListener {
      void onReady();
   }

   private enum ReadyActionStatus {
      PENDING_TX, LOCKS, PENDING_TX_TIMEOUT
   }
}
