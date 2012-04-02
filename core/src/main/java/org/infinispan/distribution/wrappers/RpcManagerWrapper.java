package org.infinispan.distribution.wrappers;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.remote.recovery.TxCompletionNotificationCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.tx.VersionedCommitCommand;
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.remoting.RpcException;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.stats.TransactionsStatisticsRegistry;
import org.infinispan.stats.translations.ExposedStatistics.IspnStats;
import org.infinispan.util.concurrent.NotifyingNotifiableFuture;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.util.Buffer;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * @author Mircea Markus <mircea.markus@jboss.com> (C) 2011 Red Hat Inc.
 * @author Diego Didona <didona@gsd.inesc-id.pt>
 * @author Pedro Ruivo
 * @since 5.2
 */
public class RpcManagerWrapper implements RpcManager {
   private static final Log log = LogFactory.getLog(RpcManagerWrapper.class);
   private final RpcManager actual;
   private final RpcDispatcher.Marshaller marshaller;

   public RpcManagerWrapper(RpcManager actual) {
      this.actual = actual;
      Transport t = actual.getTransport();
      if (t instanceof JGroupsTransport) {
         marshaller = ((JGroupsTransport) t).getCommandAwareRpcDispatcher().getMarshaller();
      } else {
         marshaller = null;
      }
   }

   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand,
                                                ResponseMode mode, long timeout, boolean usePriorityQueue,
                                                ResponseFilter responseFilter, boolean totalOrder){
      log.tracef("RpcManagerWrapper.invokeRemotely");
      long currentTime = System.nanoTime();
      Map<Address, Response> ret = actual.invokeRemotely(recipients, rpcCommand, mode, timeout, usePriorityQueue, responseFilter, totalOrder);
      updateStats(rpcCommand, mode.isSynchronous(), currentTime, recipients);
      return ret;
   }

   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand,
                                                ResponseMode mode, long timeout, boolean usePriorityQueue, boolean totalOrder) {
      log.tracef("RpcManagerWrapper.invokeRemotely_1");
      long currentTime = System.nanoTime();
      Map<Address, Response> ret = actual.invokeRemotely(recipients, rpcCommand, mode, timeout, usePriorityQueue, totalOrder);
      updateStats(rpcCommand, mode.isSynchronous(), currentTime, recipients);
      return ret;
   }

   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout, boolean totalOrder) {
      log.tracef("RpcManagerWrapper.invokeRemotely _ 2");
      long currentTime = System.nanoTime();
      Map<Address, Response> ret = actual.invokeRemotely(recipients, rpcCommand, mode, timeout, totalOrder);
      updateStats(rpcCommand, mode.isSynchronous(), currentTime, recipients);
      return ret;
   }

   @Override
   public void broadcastRpcCommand(ReplicableCommand rpc, boolean sync, boolean totalOrder) throws RpcException {
      log.tracef("RpcManagerWrapper.broadcastRpcCommand");
      long currentTime = System.nanoTime();
      actual.broadcastRpcCommand(rpc, sync, totalOrder);
      updateStats(rpc, sync, currentTime, null);
   }

   @Override
   public void broadcastRpcCommand(ReplicableCommand rpc, boolean sync, boolean usePriorityQueue, boolean totalOrder) throws RpcException {
      log.tracef("RpcManagerWrapper.broadcastRpcCommand_1");
      long currentTime = System.nanoTime();
      actual.broadcastRpcCommand(rpc, sync, usePriorityQueue, totalOrder);
      updateStats(rpc, sync, currentTime, null);
   }

   @Override
   public void broadcastRpcCommandInFuture(ReplicableCommand rpc, NotifyingNotifiableFuture<Object> future) {
      log.tracef("RpcManagerWrapper.broadcastRpcCommandInFuture");
      long currentTime = System.nanoTime();
      actual.broadcastRpcCommandInFuture(rpc, future);
      updateStats(rpc, false, currentTime, null);
   }

   @Override
   public void broadcastRpcCommandInFuture(ReplicableCommand rpc, boolean usePriorityQueue,
                                           NotifyingNotifiableFuture<Object> future) {
      log.tracef("RpcManagerWrapper.broadcastRpcCommandInFuture_1");
      long currentTime = System.nanoTime();
      actual.broadcastRpcCommandInFuture(rpc, usePriorityQueue, future);
      updateStats(rpc, false, currentTime, null);
   }

   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpc, boolean sync, boolean totalOrder) throws RpcException {
      log.tracef("RpcManagerWrapper.invokeRemotely _3");
      long currentTime = System.nanoTime();
      Map<Address, Response> ret = actual.invokeRemotely(recipients, rpc, sync, totalOrder);
      updateStats(rpc, sync, currentTime, recipients);
      return ret;
   }

   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpc, boolean sync,
                                                boolean usePriorityQueue, boolean totalOrder) throws RpcException {
      log.tracef("RpcManagerWrapper.invokeRemotely_4");
      long currentTime = System.nanoTime();
      Map<Address, Response> ret = actual.invokeRemotely(recipients, rpc, sync, usePriorityQueue, totalOrder);
      updateStats(rpc, sync, currentTime, recipients);
      return ret;
   }

   @Override
   public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpc,
                                      NotifyingNotifiableFuture<Object> future) {
      log.tracef("RpcManagerWrapper.invokeRemotelyInFuture");
      long currentTime = System.nanoTime();
      actual.invokeRemotelyInFuture(recipients, rpc, future);
      updateStats(rpc, false, currentTime, recipients);
   }

   @Override
   public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpc, boolean usePriorityQueue,
                                      NotifyingNotifiableFuture<Object> future) {
      log.tracef("RpcManagerWrapper.invokeRemotelyInFuture_1");
      long currentTime = System.nanoTime();
      actual.invokeRemotelyInFuture(recipients, rpc, usePriorityQueue, future);
      updateStats(rpc, false, currentTime, recipients);
   }

   @Override
   public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpc, boolean usePriorityQueue,
                                      NotifyingNotifiableFuture<Object> future, long timeout) {
      log.tracef("RpcManagerWrapper.invokeRemotelyInFuture_2");
      long currentTime = System.nanoTime();
      actual.invokeRemotelyInFuture(recipients, rpc, usePriorityQueue, future, timeout);
      updateStats(rpc, false, currentTime, recipients);
   }

   @Override
   public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpc, boolean usePriorityQueue,
                                      NotifyingNotifiableFuture<Object> future, long timeout, boolean ignoreLeavers) {
      log.tracef("RpcManagerWrapper.invokeRemotelyInFuture_3");
      long currentTime = System.nanoTime();
      actual.invokeRemotelyInFuture(recipients, rpc, usePriorityQueue, future, timeout, ignoreLeavers);
      updateStats(rpc, false, currentTime, recipients);
   }

   @Override
   public Transport getTransport() {
      log.tracef("RpcManagerWrapper.getTransport");
      return actual.getTransport();
   }

   @Override
   public List<Address> getMembers() {
      return actual.getMembers();
   }

   @Override
   public Address getAddress() {
      log.tracef("RpcManagerWrapper.getAddress");
      return actual.getAddress();
   }

   @Override
   public int getTopologyId() {
      return actual.getTopologyId();
   }
   
   private void updateStats(ReplicableCommand command, boolean sync, long init, Collection<Address> recipients) {
      if (!TransactionsStatisticsRegistry.hasStatisticCollector() &&
            !(command instanceof TxCompletionNotificationCommand)) {
         log.tracef("Does not update stats for command %s. No statistic collector found", command);
         return;
      }
      IspnStats durationStat;
      IspnStats counterStat;
      IspnStats recipientSizeStat;
      IspnStats commandSizeStat = null;
      switch (command.getCommandId()) {
         case PrepareCommand.COMMAND_ID:
         case VersionedPrepareCommand.COMMAND_ID:
            if (sync) {
               durationStat = IspnStats.RTT_PREPARE;
               counterStat = IspnStats.NUM_RTTS_PREPARE;
            } else {
               durationStat = IspnStats.ASYNC_PREPARE;
               counterStat = IspnStats.NUM_ASYNC_PREPARE;
            }
            recipientSizeStat = IspnStats.NUM_NODES_PREPARE;
            commandSizeStat = IspnStats.PREPARE_COMMAND_SIZE;
            break;
         case RollbackCommand.COMMAND_ID:
            if (sync) {
               durationStat = IspnStats.RTT_ROLLBACK;
               counterStat = IspnStats.NUM_RTTS_ROLLBACK;
            } else {
               durationStat = IspnStats.ASYNC_ROLLBACK;
               counterStat = IspnStats.NUM_ASYNC_ROLLBACK;
            }
            recipientSizeStat = IspnStats.NUM_NODES_ROLLBACK;
            break;
         case CommitCommand.COMMAND_ID:
         case VersionedCommitCommand.COMMAND_ID:
            if (sync) {
               durationStat = IspnStats.RTT_COMMIT;
               counterStat = IspnStats.NUM_RTTS_COMMIT;
            } else {
               durationStat = IspnStats.ASYNC_COMMIT;
               counterStat = IspnStats.NUM_ASYNC_COMMIT;
            }
            recipientSizeStat = IspnStats.NUM_NODES_COMMIT;
            commandSizeStat = IspnStats.COMMIT_COMMAND_SIZE;
            break;
         case TxCompletionNotificationCommand.COMMAND_ID:
            durationStat = IspnStats.ASYNC_COMPLETE_NOTIFY;
            counterStat = IspnStats.NUM_ASYNC_COMPLETE_NOTIFY;
            recipientSizeStat = IspnStats.NUM_NODES_COMPLETE_NOTIFY;

            log.tracef("Update stats for command %s. Is sync? %s. Duration stat is %s, counter stats is %s, " +
                             "recipient size stat is %s", command, durationStat, counterStat, recipientSizeStat);

            TransactionsStatisticsRegistry.addValueAndFlushIfNeeded(durationStat, System.nanoTime() - init, true);
            TransactionsStatisticsRegistry.incrementValueAndFlushIfNeeded(counterStat, true);
            TransactionsStatisticsRegistry.addValueAndFlushIfNeeded(recipientSizeStat, recipientListSize(recipients), true);

            return;
         case ClusteredGetCommand.COMMAND_ID:
            durationStat = IspnStats.RTT_GET;
            counterStat = IspnStats.NUM_RTTS_GET;
            recipientSizeStat = IspnStats.NUM_NODES_GET;
            commandSizeStat = IspnStats.CLUSTERED_GET_COMMAND_SIZE;
            break;
         default:
            log.tracef("Does not update stats for command %s. The command is not needed", command);
            return;
      }
      log.tracef("Update stats for command %s. Is sync? %s. Duration stat is %s, counter stats is %s, " +
                       "recipient size stat is %s", command, sync, durationStat, counterStat, recipientSizeStat);
      TransactionsStatisticsRegistry.addValue(durationStat, System.nanoTime() - init);
      TransactionsStatisticsRegistry.incrementValue(counterStat);
      TransactionsStatisticsRegistry.addValue(recipientSizeStat, recipientListSize(recipients));
      if (commandSizeStat != null) {
         TransactionsStatisticsRegistry.addValue(commandSizeStat, getCommandSize(command));
      }
   }

   private int recipientListSize(Collection<Address> recipients) {
      return recipients == null ? actual.getTransport().getMembers().size() : recipients.size();
   }

   private int getCommandSize(ReplicableCommand command) {
      try {
         Buffer buffer = marshaller.objectToBuffer(command);
         return buffer != null ? buffer.getLength() : 0;
      } catch (Exception e) {
         return 0;
      }
   }
}
