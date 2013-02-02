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
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.statetransfer.TransactionInfo;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.RemoteTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;

import java.util.Collection;

/**
 * This class is responsible to validate transactions in the total order based protocol. It ensures the delivered order
 * and will validate multiple transactions in parallel if they are non conflicting transaction.
 *
 * @author Pedro Ruivo
 * @author mircea.markus@jboss.com
 * @since 5.2.0
 */
public interface TotalOrderManager {

   /**
    * Processes the transaction as received from the sequencer.
    */
   void processTransactionFromSequencer(PrepareCommand prepareCommand, TxInvocationContext ctx, CommandInterceptor invoker) throws Exception;

   /**
    * This will mark a global transaction as finished. It will be invoked in the processing of the commit command in
    * repeatable read with write skew (not implemented yet!)
    */
   void finishTransaction(RemoteTransaction transaction, boolean commit);

   void addTransactions(Collection<TransactionInfo> pendingTransactions);

   void notifyTransactionTransferStart();

   void notifyTransactionTransferEnd();
}
