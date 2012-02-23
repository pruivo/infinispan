/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
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
package org.infinispan.transaction;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.statetransfer.TransactionInfo;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.InvalidTransactionException;
import org.infinispan.util.InfinispanCollections;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * Defines the state of a remotely originated transaction.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class RemoteTransaction extends AbstractCacheTransaction implements Cloneable {

   private static final Log log = LogFactory.getLog(RemoteTransaction.class);

   /**
    * This flag can only be true for transactions received via state transfer. During state transfer we do not migrate
    * lookedUpEntries to save bandwidth. If missingLookedUpEntries is true at the time a CommitCommand is received this
    * indicates the preceding PrepareCommand was received by previous owner before state transfer but not by the
    * current owner which now has to re-execute prepare to populate lookedUpEntries (and acquire the locks).
    */
   private volatile boolean missingLookedUpEntries = false;

   //the State and the TxDependencyLatch is used by the Total Order based protocol. they are lazily created!   
   private EnumSet<State> transactionState;

   private TxDependencyLatch txDependencyLatch;

   private static enum State {
      /**
       * the prepare command was received and started the validation
       */
      PREPARING,
      /**
       * the prepare command was received and finished the validation
       */
      PREPARED,
      /**
       * the rollback command was received before the prepare command and the transaction must be aborted
       */
      ROLLBACK_ONLY,
      /**
       * the commit command was received before the prepare command and the transaction must be committed
       */
      COMMIT_ONLY
   }


   public RemoteTransaction(WriteCommand[] modifications, GlobalTransaction tx, int topologyId) {
      super(tx, topologyId);
      this.modifications = modifications == null || modifications.length == 0
            ? InfinispanCollections.<WriteCommand>emptyList()
            : Arrays.asList(modifications);
      lookedUpEntries = new HashMap<Object, CacheEntry>(this.modifications.size());
   }

   public RemoteTransaction(GlobalTransaction tx, int topologyId) {
      super(tx, topologyId);
      this.modifications = new LinkedList<WriteCommand>();
      lookedUpEntries = new HashMap<Object, CacheEntry>(2);
   }

   @Override
   public void putLookedUpEntry(Object key, CacheEntry e) {
      checkIfRolledBack();
      if (log.isTraceEnabled()) {
         log.tracef("Adding key %s to tx %s", key, getGlobalTransaction());
      }
      lookedUpEntries.put(key, e);
   }

   @Override
   public void putLookedUpEntries(Map<Object, CacheEntry> entries) {
      checkIfRolledBack();
      if (log.isTraceEnabled()) {
         log.tracef("Adding keys %s to tx %s", entries.keySet(), getGlobalTransaction());
      }
      lookedUpEntries.putAll(entries);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof RemoteTransaction)) return false;
      RemoteTransaction that = (RemoteTransaction) o;
      return tx.equals(that.tx);
   }

   @Override
   public int hashCode() {
      return tx.hashCode();
   }

   @Override
   @SuppressWarnings("unchecked")
   public Object clone() {
      try {
         RemoteTransaction dolly = (RemoteTransaction) super.clone();
         dolly.modifications = new ArrayList<WriteCommand>(modifications);
         dolly.lookedUpEntries = new HashMap<Object, CacheEntry>(lookedUpEntries);
         return dolly;
      } catch (CloneNotSupportedException e) {
         throw new IllegalStateException("Impossible!!");
      }
   }

   @Override
   public String toString() {
      return "RemoteTransaction{" +
            "modifications=" + modifications +
            ", lookedUpEntries=" + lookedUpEntries +
            ", lockedKeys=" + lockedKeys +
            ", backupKeyLocks=" + backupKeyLocks +
            ", missingLookedUpEntries=" + missingLookedUpEntries +
            ", isMarkedForRollback=" + isMarkedForRollback()+
            ", tx=" + tx +
            ", state=" + transactionState +
            ", latch=" + txDependencyLatch +
            '}';
   }

   public void setMissingLookedUpEntries(boolean missingLookedUpEntries) {
      this.missingLookedUpEntries = missingLookedUpEntries;
   }

   public boolean isMissingLookedUpEntries() {
      return missingLookedUpEntries;
   }

   private void checkIfRolledBack() {
      if (isMarkedForRollback()) {
         throw new InvalidTransactionException("This remote transaction " + getGlobalTransaction() + " is already rolled back");
      }
   }
   
   /**
    * check if the transaction is marked for rollback (by the Rollback Command)
    * 
    * @return  true if it is marked for rollback, false otherwise
    */
   public synchronized boolean isRollbackReceived() {
      checkStateCreated();
      return transactionState.contains(State.ROLLBACK_ONLY);
   }

   /**
    * check if the transaction is marked for commit (by the Commit Command)
    * 
    * @return  true if it is marked for commit, false otherwise
    */
   public synchronized boolean isCommitReceived() {
      checkStateCreated();
      return transactionState.contains(State.COMMIT_ONLY);
   }

   /**
    * mark the transaction as prepared (the validation was finished) and notify a possible pending commit or rollback
    * command
    */
   public synchronized void prepared() {
      checkStateCreated();
      transactionState.add(State.PREPARED);
      notifyAll();
   }

   /**
    * mark the transaction as preparing, blocking the commit and rollback commands until the
    * {@link #prepared()} is invoked
    */
   public synchronized void preparing() {
      checkStateCreated();
      transactionState.add(State.PREPARING);
   }

   /**
    * Commit and rollback commands invokes this method and they are blocked here if the state is PREPARING
    *
    *
    * @param commit  true if it is a commit command, false otherwise
    * @return        true if the command needs to be processed, false otherwise
    * @throws        InterruptedException when it is interrupted while waiting
    */
   public final synchronized boolean waitUntilPrepared(boolean commit) 
         throws InterruptedException {
      checkStateCreated();
      boolean result;

      State state = commit ? State.COMMIT_ONLY : State.ROLLBACK_ONLY;
      log.tracef("Current status is %s, setting status to: %s", transactionState, state);
      transactionState.add(state);
      
      if (transactionState.contains(State.PREPARED)) {
         result = true;
         log.tracef("Transaction is PREPARED");
      } else  if (transactionState.contains(State.PREPARING)) {
         wait();
         result = true;
         log.tracef("Transaction was in PREPARING state but now it is prepared");
      } else {
         log.tracef("Transaction is not delivered yet");
         result = false;
      }      
      return result;
   }

   /**
    * Gets the latch associated to this remote transaction
    * 
    * @return  the latch associated to this transaction
    */
   public final synchronized TxDependencyLatch getLatch() {
      if (txDependencyLatch == null) {
         txDependencyLatch = new TxDependencyLatch(getGlobalTransaction());
      }
      return txDependencyLatch;
   }
   
   public final synchronized boolean isFinished() {
      return transactionState.contains(State.PREPARED) && 
            (transactionState.contains(State.COMMIT_ONLY) || transactionState.contains(State.ROLLBACK_ONLY));
   }

   public final synchronized TransactionInfo createTransactionInfo() throws InterruptedException {
      int txEncodedState = 0;
      if (transactionState != null) {
         if (transactionState.contains(State.PREPARING) && !transactionState.contains(State.PREPARED)) {
            wait();
         }
         for (State state : transactionState) {
            switch (state) {
               case PREPARED:
               case COMMIT_ONLY:
               case ROLLBACK_ONLY:
                  txEncodedState |= 1 << state.ordinal();
            }
         }
      }
      Collection<WriteCommand> mods = getModifications();

      return new TransactionInfo(getGlobalTransaction(), getTopologyId(), mods.toArray(new WriteCommand[mods.size()]), null,
                                 txEncodedState, getUpdatedEntryVersions());
   }

   public final synchronized void updateIfNeeded(TransactionInfo transactionInfo) {
      if (modifications == null) {
         modifications = Arrays.asList(transactionInfo.getModifications());
      }
      checkStateCreated();
      int encodedState = transactionInfo.getState();
      for (State state : State.values()) {
         if ((encodedState & (1 >> state.ordinal())) != 0) {
            transactionState.add(state);
         }
      }
      if (getUpdatedEntryVersions() == null) {
         setUpdatedEntryVersions(transactionInfo.getVersionsMap());
      }
   }

   //WARN this should be inside a synchronized block
   private void checkStateCreated() {
      if (transactionState == null) {
         transactionState = EnumSet.noneOf(State.class);
      }
   }
}
