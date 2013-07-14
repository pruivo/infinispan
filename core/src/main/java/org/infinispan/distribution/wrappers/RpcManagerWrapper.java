/*
 * INESC-ID, Instituto de Engenharia de Sistemas e Computadores Investigação e Desevolvimento em Lisboa
 * Copyright 2013 INESC-ID and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 3.0 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.distribution.wrappers;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.remote.recovery.TxCompletionNotificationCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.GMUPrepareCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
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
   private Address myAddress;

   public RpcManagerWrapper(RpcManager actual) {
      this.actual = actual;
      Transport t = actual.getTransport();
      if (t instanceof JGroupsTransport) {
         marshaller = ((JGroupsTransport) t).getCommandAwareRpcDispatcher().getMarshaller();
      } else {
         marshaller = null;
      }
      myAddress = actual.getTransport().getAddress();
   }

   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand,
                                                ResponseMode mode, long timeout, boolean usePriorityQueue,
                                                ResponseFilter responseFilter, boolean totalOrder) {
      long currentTime = System.nanoTime();
      Map<Address, Response> ret = actual.invokeRemotely(recipients, rpcCommand, mode, timeout, usePriorityQueue, responseFilter, totalOrder);
      updateStats(rpcCommand, mode.isSynchronous(), currentTime, recipients);
      return ret;
   }

   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand,
                                                ResponseMode mode, long timeout, boolean usePriorityQueue, boolean totalOrder) {
      long currentTime = System.nanoTime();
      Map<Address, Response> ret = actual.invokeRemotely(recipients, rpcCommand, mode, timeout, usePriorityQueue, totalOrder);
      updateStats(rpcCommand, mode.isSynchronous(), currentTime, recipients);
      return ret;
   }

   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout, boolean totalOrder) {
      long currentTime = System.nanoTime();
      Map<Address, Response> ret = actual.invokeRemotely(recipients, rpcCommand, mode, timeout, totalOrder);
      updateStats(rpcCommand, mode.isSynchronous(), currentTime, recipients);
      return ret;
   }

   @Override
   public void broadcastRpcCommand(ReplicableCommand rpc, boolean sync, boolean totalOrder) throws RpcException {
      long currentTime = System.nanoTime();
      actual.broadcastRpcCommand(rpc, sync, totalOrder);
      updateStats(rpc, sync, currentTime, null);
   }

   @Override
   public void broadcastRpcCommand(ReplicableCommand rpc, boolean sync, boolean usePriorityQueue, boolean totalOrder) throws RpcException {
      long currentTime = System.nanoTime();
      actual.broadcastRpcCommand(rpc, sync, usePriorityQueue, totalOrder);
      updateStats(rpc, sync, currentTime, null);
   }

   @Override
   public void broadcastRpcCommandInFuture(ReplicableCommand rpc, NotifyingNotifiableFuture<Object> future) {
      long currentTime = System.nanoTime();
      actual.broadcastRpcCommandInFuture(rpc, future);
      updateStats(rpc, false, currentTime, null);
   }

   @Override
   public void broadcastRpcCommandInFuture(ReplicableCommand rpc, boolean usePriorityQueue,
                                           NotifyingNotifiableFuture<Object> future) {
      long currentTime = System.nanoTime();
      actual.broadcastRpcCommandInFuture(rpc, usePriorityQueue, future);
      updateStats(rpc, false, currentTime, null);
   }

   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpc, boolean sync, boolean totalOrder) throws RpcException {
      long currentTime = System.nanoTime();
      Map<Address, Response> ret = actual.invokeRemotely(recipients, rpc, sync, totalOrder);
      updateStats(rpc, sync, currentTime, recipients);
      return ret;
   }

   @Override
   //This should be the method invoked at prepareTime
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpc, boolean sync,
                                                boolean usePriorityQueue, boolean totalOrder) throws RpcException {
      boolean isPrepareCmd = rpc instanceof PrepareCommand;
      boolean sample = TransactionsStatisticsRegistry.isActive();
      //try {
         long currentTime = System.nanoTime();
         if (isPrepareCmd && sample) {
               TransactionsStatisticsRegistry.markPrepareSent();
         }
         Map<Address, Response> ret = actual.invokeRemotely(recipients, rpc, sync, usePriorityQueue, totalOrder);
         if(sample){
            updateStats(rpc, sync, currentTime, recipients);
         }
         return ret;
      //}
      /*catch (RpcException e) {
         if (isPrepareCmd && sample) {
            TransactionsStatisticsRegistry.incrementValue(IspnStats.NUM_REMOTELY_ABORTED);
         }
         throw e;
      }
      */
   }

   @Override
   public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpc,
                                      NotifyingNotifiableFuture<Object> future) {
      long currentTime = System.nanoTime();
      actual.invokeRemotelyInFuture(recipients, rpc, future);
      updateStats(rpc, false, currentTime, recipients);
   }

   @Override
   public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpc, boolean usePriorityQueue,
                                      NotifyingNotifiableFuture<Object> future) {
      long currentTime = System.nanoTime();
      actual.invokeRemotelyInFuture(recipients, rpc, usePriorityQueue, future);
      updateStats(rpc, false, currentTime, recipients);
   }

   @Override
   public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpc, boolean usePriorityQueue,
                                      NotifyingNotifiableFuture<Object> future, long timeout) {
      long currentTime = System.nanoTime();
      actual.invokeRemotelyInFuture(recipients, rpc, usePriorityQueue, future, timeout);
      updateStats(rpc, false, currentTime, recipients);
   }

   @Override
   public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpc, boolean usePriorityQueue,
                                      NotifyingNotifiableFuture<Object> future, long timeout, boolean ignoreLeavers) {
      long currentTime = System.nanoTime();
      actual.invokeRemotelyInFuture(recipients, rpc, usePriorityQueue, future, timeout, ignoreLeavers);
      updateStats(rpc, false, currentTime, recipients);
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

   private void updateStats(ReplicableCommand command, boolean sync, long init, Collection<Address> recipients) {
      if (!TransactionsStatisticsRegistry.hasStatisticCollector() &&
            !(command instanceof TxCompletionNotificationCommand)) {
         if (log.isTraceEnabled()) {
            log.tracef("Does not update stats for command %s. No statistic collector found", command);
         }
         return;
      }
      IspnStats durationStat;
      IspnStats counterStat;
      IspnStats recipientSizeStat;
      IspnStats commandSizeStat = null;
      long contactedNodes = recipientListSize(recipients);
      long contactedNodesMinusMe = recipientListSize(recipients) - (isCurrentNodeInvolved(recipients) ? 1 : 0);
      if (command instanceof PrepareCommand) {
         if (sync) {
            durationStat = IspnStats.RTT_PREPARE;
            counterStat = IspnStats.NUM_RTTS_PREPARE;
         } else {
            durationStat = IspnStats.ASYNC_PREPARE;
            counterStat = IspnStats.NUM_ASYNC_PREPARE;
         }
         recipientSizeStat = IspnStats.NUM_NODES_PREPARE;
         commandSizeStat = IspnStats.PREPARE_COMMAND_SIZE;
      } else if (command instanceof RollbackCommand) {
         if (sync) {
            durationStat = IspnStats.RTT_ROLLBACK;
            counterStat = IspnStats.NUM_RTTS_ROLLBACK;
         } else {
            durationStat = IspnStats.ASYNC_ROLLBACK;
            counterStat = IspnStats.NUM_ASYNC_ROLLBACK;
         }
         recipientSizeStat = IspnStats.NUM_NODES_ROLLBACK;
         commandSizeStat = IspnStats.ROLLBACK_COMMAND_SIZE;
      } else if (command instanceof CommitCommand) {
         if (sync) {
            durationStat = IspnStats.RTT_COMMIT;
            counterStat = IspnStats.NUM_RTTS_COMMIT;
            TransactionsStatisticsRegistry.addValue(IspnStats.SENT_SYNC_COMMIT, contactedNodesMinusMe);
         } else {
            durationStat = IspnStats.ASYNC_COMMIT;
            counterStat = IspnStats.NUM_ASYNC_COMMIT;
            TransactionsStatisticsRegistry.addValue(IspnStats.SENT_ASYNC_COMMIT, contactedNodesMinusMe);
         }
         recipientSizeStat = IspnStats.NUM_NODES_COMMIT;
         commandSizeStat = IspnStats.COMMIT_COMMAND_SIZE;


      } else if (command instanceof ClusteredGetCommand) {
         durationStat = IspnStats.RTT_GET;
         counterStat = IspnStats.NUM_RTTS_GET;
         recipientSizeStat = IspnStats.NUM_NODES_GET;
         commandSizeStat = IspnStats.CLUSTERED_GET_COMMAND_SIZE;
      } else if (command instanceof TxCompletionNotificationCommand) {
         durationStat = IspnStats.ASYNC_COMPLETE_NOTIFY;
         counterStat = IspnStats.NUM_ASYNC_COMPLETE_NOTIFY;
         recipientSizeStat = IspnStats.NUM_NODES_COMPLETE_NOTIFY;

         if (log.isTraceEnabled()) {
            log.tracef("Update stats for command %s. Is sync? %s. Duration stat is %s, counter stats is %s, " +
                             "recipient size stat is %s", command, sync, durationStat, counterStat, recipientSizeStat);
         }

         TransactionsStatisticsRegistry.addValueAndFlushIfNeeded(durationStat, System.nanoTime() - init, true);
         TransactionsStatisticsRegistry.incrementValueAndFlushIfNeeded(counterStat, true);
         TransactionsStatisticsRegistry.addValueAndFlushIfNeeded(recipientSizeStat, contactedNodes, true);

         return;
      } else {
         if (log.isTraceEnabled()) {
            log.tracef("Does not update stats for command %s. The command is not needed", command);
         }
         return;
      }
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

   private boolean isCurrentNodeInvolved(Collection<Address> recipients) {
      //If recipients is null it's either a BroadCast (or I am the only one in the cluster, which is a trivial broadcast)
      return recipients == null || recipients.contains(myAddress);
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
