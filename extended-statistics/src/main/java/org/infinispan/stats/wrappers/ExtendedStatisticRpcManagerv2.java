package org.infinispan.stats.wrappers;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commons.util.concurrent.NotifyingNotifiableFuture;
import org.infinispan.remoting.RpcException;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.rpc.RpcOptionsBuilder;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.stats.container.NetworkStatisticsContainer;
import org.infinispan.stats.logging.Log;
import org.infinispan.stats.manager.StatisticsManager;
import org.infinispan.util.TimeService;
import org.infinispan.util.logging.LogFactory;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.util.Buffer;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Takes statistics about the RPC invocations.
 *
 * @author Roberto Palmieri
 * @author Sebastiano Peluso
 * @author Diego Didona
 * @author Pedro Ruivo
 * @since 6.0
 */
public class ExtendedStatisticRpcManagerv2 implements RpcManager {
   private static final Log log = LogFactory.getLog(ExtendedStatisticRpcManagerv2.class, Log.class);
   private final RpcManager actual;
   private final StatisticsManager statisticManager;
   private final RpcDispatcher.Marshaller marshaller;
   private final TimeService timeService;

   public ExtendedStatisticRpcManagerv2(RpcManager actual, StatisticsManager statisticManager, TimeService timeService) {
      this.actual = actual;
      this.statisticManager = statisticManager;
      Transport t = actual.getTransport();
      if (t instanceof JGroupsTransport) {
         marshaller = ((JGroupsTransport) t).getCommandAwareRpcDispatcher().getMarshaller();
      } else {
         marshaller = null;
      }
      this.timeService = timeService;
   }

   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand,
                                                ResponseMode mode, long timeout, boolean usePriorityQueue,
                                                ResponseFilter responseFilter) {
      final CommandType commandType = preProcessCommand(rpcCommand, mode);

      if (commandType == null || commandType.container == null) {
         return actual.invokeRemotely(recipients, rpcCommand, mode, timeout, usePriorityQueue, responseFilter);
      }

      final long start = timeService.time();
      Map<Address, Response> ret = actual.invokeRemotely(recipients, rpcCommand, mode, timeout, usePriorityQueue,
                                                         responseFilter);
      addStats(commandType, start, recipients, rpcCommand);
      return ret;
   }

   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand,
                                                ResponseMode mode, long timeout, boolean usePriorityQueue) {
      final CommandType commandType = preProcessCommand(rpcCommand, mode);

      if (commandType == null || commandType.container == null) {
         return actual.invokeRemotely(recipients, rpcCommand, mode, timeout, usePriorityQueue);
      }

      final long start = timeService.time();
      Map<Address, Response> ret = actual.invokeRemotely(recipients, rpcCommand, mode, timeout, usePriorityQueue);
      addStats(commandType, start, recipients, rpcCommand);
      return ret;
   }

   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand,
                                                ResponseMode mode, long timeout) {
      final CommandType commandType = preProcessCommand(rpcCommand, mode);

      if (commandType == null || commandType.container == null) {
         return actual.invokeRemotely(recipients, rpcCommand, mode, timeout);
      }

      final long start = timeService.time();
      Map<Address, Response> ret = actual.invokeRemotely(recipients, rpcCommand, mode, timeout);
      addStats(commandType, start, recipients, rpcCommand);
      return ret;
   }

   @Override
   public void broadcastRpcCommand(ReplicableCommand rpcCommand, boolean sync) throws RpcException {
      final CommandType commandType = preProcessCommand(rpcCommand, sync);

      if (commandType == null || commandType.container == null) {
         actual.broadcastRpcCommand(rpcCommand, sync);
         return;
      }

      final long start = timeService.time();
      actual.broadcastRpcCommand(rpcCommand, sync);
      addStats(commandType, start, null, rpcCommand);
   }

   @Override
   public void broadcastRpcCommand(ReplicableCommand rpcCommand, boolean sync, boolean usePriorityQueue)
         throws RpcException {
      final CommandType commandType = preProcessCommand(rpcCommand, sync);

      if (commandType == null || commandType.container == null) {
         actual.broadcastRpcCommand(rpcCommand, sync, usePriorityQueue);
         return;
      }

      final long start = timeService.time();
      actual.broadcastRpcCommand(rpcCommand, sync, usePriorityQueue);
      addStats(commandType, start, null, rpcCommand);
   }

   @Override
   public void broadcastRpcCommandInFuture(ReplicableCommand rpcCommand, NotifyingNotifiableFuture<Object> future) {
      actual.broadcastRpcCommandInFuture(rpcCommand, future);
   }

   @Override
   public void broadcastRpcCommandInFuture(ReplicableCommand rpcCommand, boolean usePriorityQueue,
                                           NotifyingNotifiableFuture<Object> future) {
      actual.broadcastRpcCommandInFuture(rpcCommand, usePriorityQueue, future);
   }

   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand,
                                                boolean sync)
         throws RpcException {
      final CommandType commandType = preProcessCommand(rpcCommand, sync);

      if (commandType == null || commandType.container == null) {
         return actual.invokeRemotely(recipients, rpcCommand, sync);
      }

      final long start = timeService.time();
      Map<Address, Response> ret = actual.invokeRemotely(recipients, rpcCommand, sync);
      addStats(commandType, start, recipients, rpcCommand);
      return ret;
   }

   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand,
                                                boolean sync, boolean usePriorityQueue) throws RpcException {
      final CommandType commandType = preProcessCommand(rpcCommand, sync);

      if (commandType == null || commandType.container == null) {
         return actual.invokeRemotely(recipients, rpcCommand, sync, usePriorityQueue);
      }

      final long start = timeService.time();
      Map<Address, Response> ret = actual.invokeRemotely(recipients, rpcCommand, sync, usePriorityQueue);
      addStats(commandType, start, recipients, rpcCommand);
      return ret;
   }

   @Override
   public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpcCommand,
                                      NotifyingNotifiableFuture<Object> future) {
      actual.invokeRemotelyInFuture(recipients, rpcCommand, future);
   }

   @Override
   public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpcCommand, boolean usePriorityQueue,
                                      NotifyingNotifiableFuture<Object> future) {
      actual.invokeRemotelyInFuture(recipients, rpcCommand, usePriorityQueue, future);
   }

   @Override
   public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpcCommand, boolean usePriorityQueue,
                                      NotifyingNotifiableFuture<Object> future, long timeout) {
      actual.invokeRemotelyInFuture(recipients, rpcCommand, usePriorityQueue, future, timeout);
   }

   @Override
   public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpcCommand, boolean usePriorityQueue,
                                      NotifyingNotifiableFuture<Object> future, long timeout, boolean ignoreLeavers) {
      actual.invokeRemotelyInFuture(recipients, rpcCommand, usePriorityQueue, future, timeout, ignoreLeavers);
   }

   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand,
                                                RpcOptions options) {
      final CommandType commandType = preProcessCommand(rpcCommand, options.responseMode());

      if (commandType == null || commandType.container == null) {
         return actual.invokeRemotely(recipients, rpcCommand, options);
      }

      final long start = timeService.time();
      Map<Address, Response> ret = actual.invokeRemotely(recipients, rpcCommand, options);
      addStats(commandType, start, recipients, rpcCommand);
      return ret;
   }

   @Override
   public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpcCommand, RpcOptions options,
                                      NotifyingNotifiableFuture<Object> future) {
      actual.invokeRemotelyInFuture(recipients, rpcCommand, options, future);
   }

   @Override
   public RpcOptionsBuilder getRpcOptionsBuilder(ResponseMode responseMode) {
      return actual.getRpcOptionsBuilder(responseMode);
   }

   @Override
   public RpcOptionsBuilder getRpcOptionsBuilder(ResponseMode responseMode, boolean fifoOrder) {
      return actual.getRpcOptionsBuilder(responseMode, fifoOrder);
   }

   @Override
   public RpcOptions getDefaultRpcOptions(boolean sync) {
      return actual.getDefaultRpcOptions(sync);
   }

   @Override
   public RpcOptions getDefaultRpcOptions(boolean sync, boolean fifoOrder) {
      return actual.getDefaultRpcOptions(sync, fifoOrder);
   }

   @Override
   public Transport getTransport() {
      return actual.getTransport();
   }

   @Override
   public List<Address> getMembers() {
      return actual.getMembers();
   }

   @Override
   public Address getAddress() {
      return actual.getAddress();
   }

   @Override
   public int getTopologyId() {
      return actual.getTopologyId();
   }

   private int getInvoledNodes(Collection<Address> recipients) {
      if (recipients == null) {
         return actual.getMembers().size() - 1;
      }
      final int size = recipients.size();
      return recipients.contains(actual.getAddress()) ? size - 1 : size;
   }

   private int getCommandSize(ReplicableCommand command) {
      try {
         Buffer buffer = marshaller.objectToBuffer(command);
         return buffer != null ? buffer.getLength() : 0;
      } catch (Exception e) {
         return 0;
      }
   }

   private void addStats(CommandType type, long startTimeStamp, Collection<Address> recipients,
                         ReplicableCommand command) {
      final long rtt = timeService.timeDuration(startTimeStamp, NANOSECONDS);
      final int nrInvolvedNodes = getInvoledNodes(recipients);
      final int size = getCommandSize(command);
      switch (type.type) {
         case PREPARE:
            type.container.prepare(rtt, size, nrInvolvedNodes);
            break;
         case COMMIT:
            type.container.commit(rtt, size, nrInvolvedNodes);
            break;
         case ROLLBACK:
            type.container.rollback(rtt, size, nrInvolvedNodes);
            break;
         case GET:
            type.container.remoteGet(rtt, size, nrInvolvedNodes);
            break;
      }
   }

   private CommandType preProcessCommand(ReplicableCommand command, ResponseMode mode) {
      return preProcessCommand(command, mode.isSynchronous());
   }

   private CommandType preProcessCommand(ReplicableCommand command, boolean sync) {
      return sync ? preProcessCommand(command) : null;
   }

   private CommandType preProcessCommand(ReplicableCommand command) {
      if (command instanceof PrepareCommand) {
         return new CommandType(Type.PREPARE, statisticManager.getNetworkStatisticsContainer(
               ((PrepareCommand) command).getGlobalTransaction()));
      } else if (command instanceof CommitCommand) {
         return new CommandType(Type.COMMIT, statisticManager.getNetworkStatisticsContainer(
               ((CommitCommand) command).getGlobalTransaction()));
      } else if (command instanceof RollbackCommand) {
         return new CommandType(Type.ROLLBACK, statisticManager.getNetworkStatisticsContainer(
               ((RollbackCommand) command).getGlobalTransaction()));
      } else if (command instanceof ClusteredGetCommand) {
         return new CommandType(Type.GET, statisticManager.getNetworkStatisticsContainer(
               ((ClusteredGetCommand) command).getGlobalTransaction()));
      }
      return null;
   }

   private enum Type {
      PREPARE, COMMIT, ROLLBACK, GET
   }

   private class CommandType {
      private final Type type;
      private final NetworkStatisticsContainer container;

      private CommandType(Type type, NetworkStatisticsContainer container) {
         this.type = type;
         this.container = container;
      }
   }
}
