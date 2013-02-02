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

import org.infinispan.transaction.RemoteTransaction;
import org.infinispan.transaction.TxDependencyLatch;
import org.infinispan.transaction.xa.GlobalTransaction;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class StateTransferState {

   //pending transactions from state transfer
   private final Set<GlobalTransaction> pendingTransactions;
   private final Map<Object, Set<TxDependencyLatch>> pendingLockedKeys;
   private final Set<TxDependencyLatch> clearCommands;
   private final List<GlobalTransaction> toRemove;
   private boolean canRemove;

   public StateTransferState() {
      pendingLockedKeys = new HashMap<Object, Set<TxDependencyLatch>>();
      pendingTransactions = new HashSet<GlobalTransaction>();
      clearCommands = new HashSet<TxDependencyLatch>();
      toRemove = new LinkedList<GlobalTransaction>();
   }

   public final synchronized void addTransaction(RemoteTransaction remoteTransaction, Object[] keys) {
      canRemove = false;
      GlobalTransaction globalTransaction = remoteTransaction.getGlobalTransaction();
      if (pendingTransactions.add(globalTransaction)) {
         addPendingLockedKeys(keys, remoteTransaction.getLatch());
      }
   }

   public final synchronized void transactionTransferStart() {
      canRemove = false;
   }

   public final synchronized void transactionTransferEnd() {
      canRemove = true;
      pendingTransactions.removeAll(toRemove);
      toRemove.clear();
      checkPendingLocks();
   }

   public final synchronized void transactionCompleted(GlobalTransaction globalTransaction) {
      if (canRemove) {
         pendingTransactions.remove(globalTransaction);
         checkPendingLocks();
      } else {
         toRemove.add(globalTransaction);
      }
   }

   public final synchronized Collection<TxDependencyLatch> getDependencyLatches(Object[] keys) {
      Set<TxDependencyLatch> txDependencyLatchSet = new HashSet<TxDependencyLatch>();
      txDependencyLatchSet.addAll(clearCommands);

      if (keys == null) {
         for (Collection<TxDependencyLatch> latchCollection : pendingLockedKeys.values()) {
            txDependencyLatchSet.addAll(latchCollection);
         }
         return txDependencyLatchSet;
      }

      for (Object key : keys) {
         Collection<TxDependencyLatch> keyLatchCollection = pendingLockedKeys.get(key);
         if (keyLatchCollection != null) {
            txDependencyLatchSet.addAll(pendingLockedKeys.get(key));
         }
      }
      return txDependencyLatchSet;
   }

   private void checkPendingLocks() {
      if (pendingTransactions.isEmpty()) {
         pendingLockedKeys.clear();
         clearCommands.clear();
      }
   }

   private void addPendingLockedKeys(Object[] keys, TxDependencyLatch latch) {
      if (keys == null) {
         clearCommands.add(latch);
         return;
      }

      for (Object key : keys) {
         Set<TxDependencyLatch> latchSet = pendingLockedKeys.get(key);
         if (latchSet == null) {
            latchSet = new HashSet<TxDependencyLatch>();
            pendingLockedKeys.put(key, latchSet);
         }
         latchSet.add(latch);
      }
   }

}
