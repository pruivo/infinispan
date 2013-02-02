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
package org.infinispan.transaction.gmu.manager;

import org.infinispan.commands.tx.GMUCommitCommand;
import org.infinispan.container.versioning.gmu.GMUVersion;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static org.infinispan.transaction.gmu.GMUHelper.toGMUVersion;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class SortedTransactionQueue {

   private static final Log log = LogFactory.getLog(SortedTransactionQueue.class);

   private final ConcurrentHashMap<GlobalTransaction, Node> concurrentHashMap;
   private final Node firstEntry;
   private final Node lastEntry;

   public SortedTransactionQueue() {
      this.concurrentHashMap = new ConcurrentHashMap<GlobalTransaction, Node>();

      this.firstEntry = new Node() {
         private Node first;

         @Override
         public void commitVersion(GMUVersion commitCommand) {}

         @Override
         public GMUVersion getVersion() {
            throw new IllegalStateException("Cannot return the version from the first node");
         }

         @Override
         public boolean isReady() {
            return false;
         }

         @Override
         public boolean isCommitted() {
            return false;
         }

         @Override
         public GlobalTransaction getGlobalTransaction() {
            throw new IllegalStateException("Cannot return the global transaction from the first node");
         }

         @Override
         public Node getPrevious() {
            throw new IllegalStateException("Cannot return the previous node from the first node");
         }

         @Override
         public void setPrevious(Node previous) {
            throw new IllegalStateException("Cannot set the previous node from the first node");
         }

         @Override
         public Node getNext() {
            return first;
         }

         @Override
         public void setNext(Node next) {
            this.first = next;
         }

         @Override
         public int compareTo(Node o) {
            //the first node is always lower
            return -1;
         }

         @Override
         public void awaitUntilCommitted(GMUCommitCommand commitCommand) throws InterruptedException {}

         @Override
         public CacheTransaction getCacheTransactionForCommit() {
            throw new IllegalStateException("Cannot return the cache transaction from the first node");
         }

         @Override
         public void committed() {}

         @Override
         public String toString() {
            return "FIRST_ENTRY";
         }
      };

      this.lastEntry = new Node() {
         private Node last;

         @Override
         public void commitVersion(GMUVersion commitCommand) {}

         @Override
         public GMUVersion getVersion() {
            throw new IllegalStateException("Cannot return the version from the last node");
         }

         @Override
         public boolean isReady() {
            return false;
         }

         @Override
         public boolean isCommitted() {
            return false;
         }

         @Override
         public GlobalTransaction getGlobalTransaction() {
            throw new IllegalStateException("Cannot return the global transaction from the last node");
         }

         @Override
         public Node getPrevious() {
            return last;
         }

         @Override
         public void setPrevious(Node previous) {
            this.last = previous;
         }

         @Override
         public Node getNext() {
            throw new IllegalStateException("Cannot return the next node from the last node");
         }

         @Override
         public void setNext(Node next) {
            throw new IllegalStateException("Cannot set the next node from the last node");
         }

         @Override
         public int compareTo(Node o) {
            //the last node is always higher
            return 1;
         }

         @Override
         public void awaitUntilCommitted(GMUCommitCommand commitCommand) throws InterruptedException {}

         @Override
         public CacheTransaction getCacheTransactionForCommit() {
            throw new IllegalStateException("Cannot return the cache transaction from the last node");
         }

         @Override
         public void committed() {}

         @Override
         public String toString() {
            return "LAST_ENTRY";
         }
      };

      firstEntry.setNext(lastEntry);
      lastEntry.setPrevious(firstEntry);
   }

   public final void prepare(CacheTransaction cacheTransaction) {
      GlobalTransaction globalTransaction = cacheTransaction.getGlobalTransaction();
      if (concurrentHashMap.contains(globalTransaction)) {
         log.warnf("Duplicated prepare for %s", globalTransaction);
      }
      Node entry = new TransactionEntryImpl(cacheTransaction);
      concurrentHashMap.put(globalTransaction, entry);
      addNew(entry);
   }

   public final void rollback(CacheTransaction cacheTransaction) {
      remove(concurrentHashMap.remove(cacheTransaction.getGlobalTransaction()));
      notifyIfNeeded();
   }

   //return true if it is a read-write transaction
   public final boolean commit(CacheTransaction cacheTransaction, GMUVersion commitVersion) {
      Node entry = concurrentHashMap.get(cacheTransaction.getGlobalTransaction());
      if (entry == null) {
         if (log.isDebugEnabled()) {
            log.debugf("Cannot commit transaction %s. Maybe it is a read-only on this node",
                       cacheTransaction.getGlobalTransaction().prettyPrint());
         }
         return false;
      }
      update(entry, commitVersion);
      notifyIfNeeded();
      return true;
   }

   public final synchronized void populateToCommit(List<TransactionEntry> transactionEntryList) throws InterruptedException {
      removeCommitted();
      while (!firstEntry.getNext().isReady()) {
         if (log.isTraceEnabled()) {
            log.tracef("get transactions to commit. First is not ready! %s", firstEntry.getNext());
         }
         wait();
      }

      //if (log.isDebugEnabled()) {
      //   log.debugf("Try to commit transaction. Queue is %s", queueToString());
      //}

      Node firstTransaction = firstEntry.getNext();

      Node transactionToCheck = firstTransaction.getNext();

      while (transactionToCheck != lastEntry) {
         boolean isSameVersion = transactionToCheck.compareTo(firstTransaction) == 0;
         if (!isSameVersion) {
            //commit until this transaction
            commitUntil(transactionToCheck, transactionEntryList);
            return;
         } else if (!transactionToCheck.isReady()) {
            if (log.isTraceEnabled()) {
               log.tracef("Transaction with the same version not ready. %s and %s", firstTransaction, transactionToCheck);
            }
            wait();
            return;
         }
         transactionToCheck = transactionToCheck.getNext();
      }
      //commit until this transaction
      commitUntil(transactionToCheck, transactionEntryList);
   }

   public final TransactionEntry getTransactionEntry(GlobalTransaction globalTransaction) {
      return concurrentHashMap.get(globalTransaction);
   }

   public final synchronized String queueToString() {
      Node node = firstEntry.getNext();
      if (node == lastEntry) {
         return "[]";
      }
      StringBuilder builder = new StringBuilder("[");
      builder.append(node);

      node = node.getNext();
      while (node != lastEntry) {
         builder.append(",").append(node);
         node.getNext();
      }
      builder.append("]");
      return builder.toString();
   }

   private void commitUntil(Node exclusive, List<TransactionEntry> transactionEntryList) {
      Node transaction = firstEntry.getNext();

      while (transaction != exclusive) {
         transactionEntryList.add(transaction);
         transaction = transaction.getNext();
      }
   }

   private void removeCommitted() {
      Node node = firstEntry.getNext();
      while (node != lastEntry) {
         if (node.isCommitted()) {
            node = node.getNext();
         } else {
            break;
         }
      }
      Node newFirst = node;
      node = newFirst.getPrevious();
      while (node != firstEntry) {
         node.getNext().setPrevious(null);
         node.setNext(null);
         node = node.getPrevious();
      }
      firstEntry.setNext(newFirst);
      newFirst.setPrevious(firstEntry);
   }

   private synchronized void update(Node entry, GMUVersion commitVersion) {
      if (log.isTraceEnabled()) {
         log.tracef("Update %s with %s", entry, commitVersion);
      }

      entry.commitVersion(commitVersion);
      if (entry.compareTo(entry.getNext()) > 0) {
         Node insertBefore = entry.getNext().getNext();
         remove(entry);
         while (entry.compareTo(insertBefore) > 0) {
            insertBefore = insertBefore.getNext();
         }
         addBefore(insertBefore, entry);
      }
   }

   private synchronized void addNew(Node entry) {
      if (log.isTraceEnabled()) {
         log.tracef("Add new entry: %s", entry);
      }
      Node insertAfter = lastEntry.getPrevious();

      while (insertAfter != firstEntry) {
         if (insertAfter.compareTo(entry) <= 0) {
            break;
         }
         insertAfter = insertAfter.getPrevious();
      }
      addAfter(insertAfter, entry);
      if (log.isTraceEnabled()) {
         log.tracef("After add, first entry is %s", firstEntry.getNext());
      }
   }

   private synchronized void remove(Node entry) {
      if (entry == null) {
         return;
      }

      if (log.isTraceEnabled()) {
         log.tracef("remove entry: %s", entry);
      }

      Node previous = entry.getPrevious();
      Node next = entry.getNext();
      entry.setPrevious(null);
      entry.setNext(null);

      previous.setNext(next);
      next.setPrevious(previous);
      if (log.isTraceEnabled()) {
         log.tracef("After remove, first entry is %s", firstEntry.getNext());
      }
   }

   private synchronized void addAfter(Node insertAfter, Node entry) {
      entry.setNext(insertAfter.getNext());
      insertAfter.getNext().setPrevious(entry);

      entry.setPrevious(insertAfter);
      insertAfter.setNext(entry);

      if (log.isTraceEnabled()) {
         log.tracef("add after: %s --> [%s] --> %s", insertAfter, entry, entry.getNext());
      }
   }

   private void addBefore(Node insertBefore, Node entry) {
      entry.setPrevious(insertBefore.getPrevious());
      insertBefore.getPrevious().setNext(entry);

      entry.setNext(insertBefore);
      insertBefore.setPrevious(entry);
      if (log.isTraceEnabled()) {
         log.tracef("add before: %s --> [%s] --> %s", entry.getPrevious(), entry, insertBefore);
      }
   }

   private synchronized void notifyIfNeeded() {
      if (firstEntry.getNext().isReady()) {
         notify();
      }
   }

   private class TransactionEntryImpl implements Node {

      private final CacheTransaction cacheTransaction;
      private GMUVersion entryVersion;
      private boolean ready;
      private boolean committed;
      private GMUCommitCommand commitCommand;

      private Node previous;
      private Node next;

      private TransactionEntryImpl(CacheTransaction cacheTransaction) {
         this.cacheTransaction = cacheTransaction;
         this.entryVersion = toGMUVersion(cacheTransaction.getTransactionVersion());
      }

      public synchronized void commitVersion(GMUVersion commitVersion) {
         this.entryVersion = commitVersion;
         this.ready = true;
         if (log.isTraceEnabled()) {
            log.tracef("Set transaction commit version: %s", this);
         }
      }

      public synchronized GMUVersion getVersion() {
         return entryVersion;
      }

      public CacheTransaction getCacheTransaction() {
         return cacheTransaction;
      }

      public synchronized boolean isReady() {
         return ready;
      }

      @Override
      public synchronized boolean isCommitted() {
         return committed;
      }

      public GlobalTransaction getGlobalTransaction() {
         return cacheTransaction.getGlobalTransaction();
      }

      public synchronized void committed() {
         if (log.isTraceEnabled()) {
            log.tracef("Mark transaction committed: %s", this);
         }
         committed = true;
         if (commitCommand != null) {
            commitCommand.sendReply(null, false);
         }
         notifyAll();
      }

      @Override
      public synchronized void awaitUntilCommitted(GMUCommitCommand commitCommand) throws InterruptedException {
         if (log.isTraceEnabled()) {
            log.tracef("await until this [%s] is committed.", this);
         }
         if (committed && commitCommand != null) {
            commitCommand.sendReply(null, false);
            if (log.isTraceEnabled()) {
               log.tracef("Done! This [%s] is committed.", this);
            }
            return;
         }
         if (commitCommand != null) {
            this.commitCommand = commitCommand;
            if (log.isTraceEnabled()) {
               log.tracef("Don't wait. It is remote. Reply will be sent when this [%s] is committed.", this);
            }
            return;
         }
         while (!committed) {
            wait();
         }
         if (log.isTraceEnabled()) {
            log.tracef("Done! This [%s] is committed.", this);
         }
      }

      @Override
      public CacheTransaction getCacheTransactionForCommit() {
         cacheTransaction.setTransactionVersion(entryVersion);
         return cacheTransaction;
      }

      @Override
      public String toString() {
         return "TransactionEntry{" +
               "version=" + getVersion() +
               ", ready=" + isReady() +
               ", gtx=" + cacheTransaction.getGlobalTransaction().prettyPrint() +
               '}';
      }

      @Override
      public int compareTo(Node otherNode) {
         if (otherNode == null) {
            return -1;
         } else if (otherNode == firstEntry) {
            return 1;
         } else if (otherNode == lastEntry) {
            return -1;
         }

         Long my = getVersion().getThisNodeVersionValue();
         Long other = otherNode.getVersion().getThisNodeVersionValue();
         int compareResult = my.compareTo(other);

         if (log.isTraceEnabled()) {
            log.tracef("Comparing this[%s] with other[%s]. compare(%s,%s) ==> %s", this, otherNode, my, other,
                       compareResult);
         }

         return compareResult;
      }

      @Override
      public Node getPrevious() {
         return previous;
      }

      @Override
      public void setPrevious(Node previous) {
         this.previous = previous;
      }

      @Override
      public Node getNext() {
         return next;
      }

      @Override
      public void setNext(Node next) {
         this.next = next;
      }
   }

   private interface Node extends TransactionEntry, Comparable<Node> {
      void commitVersion(GMUVersion commitCommand);
      GMUVersion getVersion();
      boolean isReady();
      boolean isCommitted();
      GlobalTransaction getGlobalTransaction();

      Node getPrevious();
      void setPrevious(Node previous);

      Node getNext();
      void setNext(Node next);
   }

   public static interface TransactionEntry {
      void awaitUntilCommitted(GMUCommitCommand commitCommand) throws InterruptedException;
      CacheTransaction getCacheTransactionForCommit();
      void committed();
   }
}
