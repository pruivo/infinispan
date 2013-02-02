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
package org.infinispan.transaction.totalorder;

import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.statetransfer.TransactionInfo;
import org.infinispan.transaction.RemoteTransaction;

import java.util.Collection;
import java.util.HashSet;

/**
 * @author mircea.markus@jboss.com
 * @author Pedro Ruivo
 * @since 5.2.0
 */
@MBean(objectName = "SequentialTotalOrderManager", description = "Simple total order management")
public class SequentialTotalOrderManager extends BaseTotalOrderManager {

   @Override
   public final void processTransactionFromSequencer(PrepareCommand prepareCommand, TxInvocationContext ctx, CommandInterceptor invoker) {

      logAndCheckContext(prepareCommand, ctx);

      copyLookedUpEntriesToRemoteContext(ctx);

      try {
         awaitIncomingStateTransfer(ctx.getModifications());
      } catch (InterruptedException e) {
         log.warn("Interrupted while waiting for incoming segments...");
         Thread.currentThread().interrupt();
         return;
      }

      Object result = null;
      boolean exceptionThrown = false;
      long startTime = now();
      try {
         result = prepareCommand.acceptVisitor(ctx, invoker);
      } catch (Throwable exception) {
         exceptionThrown = true;
         result = exception;
      } finally {
         if (result instanceof EntryVersionsMap) {
            //this is not an exception
            prepareCommand.sendReply(new HashSet<Object>(((EntryVersionsMap) result).keySet()), false);
         } else {
            prepareCommand.sendReply(result, exceptionThrown);
         }

         transactionCompleted(prepareCommand.getGlobalTransaction(), !exceptionThrown);
         updateProcessingDurationStats(startTime, now());
      }
   }

   @Override
   public void addTransactions(Collection<TransactionInfo> pendingTransactions) {
      //no-op, one phase commit does not have pending transactions
   }

   @Override
   public void notifyTransactionTransferStart() {
      //no-op, one phase commit does not have pending transactions
   }

   @Override
   public void notifyTransactionTransferEnd() {
      //no-op, one phase commit does not have pending transactions
   }

   @Override
   protected void releaseResources(RemoteTransaction remoteTransaction) {
      //nothing to release because nothing is allocated
   }

   /**
    * update statistics if enabled. all the time are in nanoseconds
    *
    * @param start   the start time
    * @param end     the end time
    */
   private void updateProcessingDurationStats(long start, long end) {
      if (statisticsEnabled) {
         processingDuration.addAndGet(end - start);
         numberOfTxValidated.incrementAndGet();
      }
   }
}
