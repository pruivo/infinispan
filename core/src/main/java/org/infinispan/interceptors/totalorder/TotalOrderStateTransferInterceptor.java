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
package org.infinispan.interceptors.totalorder;

import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.remoting.RpcException;
import org.infinispan.statetransfer.StateTransferInterceptor;
import org.infinispan.topology.CacheTopology;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.RemoteTransaction;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class TotalOrderStateTransferInterceptor extends StateTransferInterceptor {

   private final StateTransferException RETRY_EXCEPTION = new StateTransferException("Retry exception");

   private static final Log log = LogFactory.getLog(TotalOrderStateTransferInterceptor.class);

   private TransactionTable transactionTable;

   @Inject
   public void inject(TransactionTable transactionTable) {
      this.transactionTable = transactionTable;
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      if (ctx.isOriginLocal()) {
         return localPrepare(ctx, command);
      }
      return remotePrepare(ctx, command);
   }

   private Object remotePrepare(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      CacheTopology cacheTopology = stateTransferManager.getCacheTopology();
      final int topologyId = cacheTopology.getTopologyId();
      ((RemoteTransaction) ctx.getCacheTransaction()).setMissingLookedUpEntries(false);

      if (log.isTraceEnabled()) {
         log.tracef("Remote transaction received %s. Tx topology id is %s and current topology is is %s",
                    ctx.getGlobalTransaction().prettyPrint(), command.getTopologyId(), topologyId);
      }

      stateTransferLock.waitForTransactionData(command.getTopologyId());

      if (command.getTopologyId() < topologyId) {
         LocalTransaction localTransaction = transactionTable.getLocalTransaction(command.getGlobalTransaction());
         if (localTransaction != null) {
            throw RETRY_EXCEPTION;
         }
         if (log.isDebugEnabled()) {
            log.debugf("Transaction %s delivered in new topology Id. Discard it because it should be retransmitted",
                       ctx.getGlobalTransaction().prettyPrint());
         }
         //discard the prepare
         return null;
      } else if (command.getTopologyId() > topologyId) {
         throw new IllegalStateException("This should never happen");
      } else if (!stateTransferManager.hasReceivedInitialState()) {
         if (log.isDebugEnabled()) {
            log.debugf("Transaction %s delivered in joiner without the initial state. Discard it",
                       ctx.getGlobalTransaction().prettyPrint());
         }
         //discard, the state transfer will bring the data and the tx coordinator does not expect any reply from
         //this node
         transactionTable.removeRemoteTransaction(command.getGlobalTransaction());
         return null;
      }
      return invokeNextInterceptor(ctx, command);
   }

   private Object localPrepare(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      boolean needsToPrepare = true;
      Object retVal = null;
      while (needsToPrepare) {
         try {
            CacheTopology cacheTopology = stateTransferManager.getCacheTopology();

            command.setTopologyId(cacheTopology.getTopologyId());

            if (log.isTraceEnabled()) {
               log.tracef("Local transaction received %s. setting topology Id to %s",
                          command.getGlobalTransaction().prettyPrint(), command.getTopologyId());
            }

            retVal = invokeNextInterceptor(ctx, command);
            needsToPrepare = false;
         } catch (RpcException rpcException) {
            needsToPrepare = rpcException.getCause() == RETRY_EXCEPTION;
            if (log.isDebugEnabled()) {
               log.tracef("Exception caught while preparing transaction %s (cause = %s). Needs to retransmit? %s",
                          command.getGlobalTransaction().prettyPrint(), rpcException.getCause(), needsToPrepare);
            }

            if (!needsToPrepare) {
               throw rpcException;
            }
         }
      }
      return retVal;
   }
}
