package org.infinispan.transaction.gmu.manager;

import org.infinispan.container.versioning.gmu.GMUVersion;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

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

      this.firstEntry = new AbstractBoundaryNode() {
         private Node first;

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
         public String toString() {
            return "FIRST_ENTRY";
         }
      };

      this.lastEntry = new AbstractBoundaryNode() {
         private Node last;

         @Override
         public Node getPrevious() {
            return last;
         }

         @Override
         public void setPrevious(Node previous) {
            this.last = previous;
         }

         @Override
         public int compareTo(Node o) {
            //the last node is always higher
            return 1;
         }

         @Override
         public String toString() {
            return "LAST_ENTRY";
         }
      };

      firstEntry.setNext(lastEntry);
      lastEntry.setPrevious(firstEntry);
   }

   public final void prepare(CacheTransaction cacheTransaction, long concurrentClockNumber) {
      GlobalTransaction globalTransaction = cacheTransaction.getGlobalTransaction();
      if (concurrentHashMap.contains(globalTransaction)) {
         log.warnf("Duplicated prepare for %s", globalTransaction);
      }
      Node entry = new TransactionEntryImpl(cacheTransaction, concurrentClockNumber);
      concurrentHashMap.put(globalTransaction, entry);
      addNew(entry);
   }

   public final void rollback(CacheTransaction cacheTransaction) {
      remove(concurrentHashMap.remove(cacheTransaction.getGlobalTransaction()));
   }

   //return true if it is a read-write transaction
   public final TransactionEntry commit(GlobalTransaction globalTransaction, GMUVersion commitVersion) {
      Node entry = concurrentHashMap.get(globalTransaction);
      if (entry == null) {
         if (log.isDebugEnabled()) {
            log.debugf("Cannot commit transaction %s. Maybe it is a read-only on this node",
                       globalTransaction.globalId());
         }
         return entry;
      }
      update(entry, commitVersion);
      return entry;
   }

   public final synchronized void populateToCommit(List<TransactionEntry> transactionEntryList) {
      removeCommitted();
      if (!firstEntry.getNext().hasReceiveCommitCommand()) {
         if (log.isTraceEnabled()) {
            log.tracef("get transactions to commit. First is not ready! %s", firstEntry.getNext());
         }
         return;
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
         } else if (!transactionToCheck.hasReceiveCommitCommand()) {
            if (log.isTraceEnabled()) {
               log.tracef("Transaction with the same version not ready. %s and %s", firstTransaction, transactionToCheck);
            }
            return;
         }
         transactionToCheck = transactionToCheck.getNext();
      }
      //commit until this transaction
      commitUntil(transactionToCheck, transactionEntryList);
   }

   /**
    * @return {@code true} if it has already transactions to be committed
    */
   public final synchronized boolean hasTransactionReadyToCommit() {
      removeCommitted();
      //same code as populateToCommit...
      if (!firstEntry.getNext().hasReceiveCommitCommand()) {
         if (log.isTraceEnabled()) {
            log.tracef("hasTransactionReadyToCommit() == false. %s", firstEntry.getNext());
         }
         return false;
      }

      Node firstTransaction = firstEntry.getNext();

      Node transactionToCheck = firstTransaction.getNext();

      while (transactionToCheck != lastEntry) {
         boolean isSameVersion = transactionToCheck.compareTo(firstTransaction) == 0;
         if (!isSameVersion) {
            if (log.isTraceEnabled()) {
               log.tracef("hasTransactionReadyToCommit() == true. %s", firstTransaction);
            }
            firstTransaction.markReadyToCommit();
            return true;
         } else if (!transactionToCheck.hasReceiveCommitCommand()) {
            if (log.isTraceEnabled()) {
               log.tracef("hasTransactionReadyToCommit() == false. %s is waiting for", firstTransaction, transactionToCheck);
            }
            return false;
         }
         transactionToCheck = transactionToCheck.getNext();
      }
      if (log.isTraceEnabled()) {
         log.tracef("hasTransactionReadyToCommit() == true. %s", firstTransaction);
      }
      firstTransaction.markReadyToCommit();
      return true;
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
            node.markReadyToCommit(); //to be sure that is unblocked
            node = node.getNext();
         } else {
            break;
         }
      }
      Node newFirst = node;
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

   private interface Node extends TransactionEntry, Comparable<Node> {
      /**
       * adds the commit entry
       *
       * @param commitCommand commit command
       */
      void commitVersion(GMUVersion commitCommand);

      /**
       * @return the current version
       */
      GMUVersion getVersion();

      /**
       * @return {@code true} if the transaction has already received the commit command
       */
      boolean hasReceiveCommitCommand();

      /**
       * marks the transaction ready to commit. This means that {@link #hasReceiveCommitCommand()} is true and it is the
       * first entry in the queue
       */
      void markReadyToCommit();

      Node getPrevious();

      void setPrevious(Node previous);

      Node getNext();

      void setNext(Node next);
   }

   public static interface TransactionEntry {
      /**
       * waits until this entry if the first in the queue
       *
       * @throws InterruptedException
       */
      void awaitUntilIsReadyToCommit() throws InterruptedException;

      /**
       * @return {@code true} if this transaction has receive the commit command and it is the first in the queue
       */
      boolean isReadyToCommit();

      /**
       * @return {@link CacheTransaction} with the version updated
       */
      CacheTransaction getCacheTransactionForCommit();

      /**
       * marks this entry has committed, i.e. the modifications have been flushed in the data container
       */
      void committed();

      /**
       * @return {@code true} if this entry was committed
       */
      boolean isCommitted();

      GlobalTransaction getGlobalTransaction();

      void awaitUntilCommitted() throws InterruptedException;

      long getConcurrentClockNumber();
   }

   private class TransactionEntryImpl implements Node {

      private final CacheTransaction cacheTransaction;
      private final CountDownLatch readyToCommit;
      private volatile GMUVersion entryVersion;
      private volatile boolean receivedCommitCommand;
      private volatile boolean committed;
      private long concurrentClockNumber; //This is the value of the last committed vector clock's n-th entry on this node n at the time this object was created.

      private Node previous;
      private Node next;

      private TransactionEntryImpl(CacheTransaction cacheTransaction, long concurrentClockNumber) {
         this.cacheTransaction = cacheTransaction;
         this.entryVersion = toGMUVersion(cacheTransaction.getTransactionVersion());
         this.readyToCommit = new CountDownLatch(1);
         this.concurrentClockNumber = concurrentClockNumber;
      }

      public void commitVersion(GMUVersion commitVersion) {
         this.entryVersion = commitVersion;
         this.receivedCommitCommand = true;
         if (log.isTraceEnabled()) {
            log.tracef("Set transaction commit version: %s", this);
         }
      }

      public synchronized GMUVersion getVersion() {
         return entryVersion;
      }

      public synchronized long getConcurrentClockNumber(){
         return concurrentClockNumber;
      }

      public CacheTransaction getCacheTransaction() {
         return cacheTransaction;
      }

      public boolean hasReceiveCommitCommand() {
         return receivedCommitCommand;
      }

      @Override
      public void markReadyToCommit() {
         readyToCommit.countDown();
      }

      @Override
      public boolean isCommitted() {
         return committed;
      }

      public GlobalTransaction getGlobalTransaction() {
         return cacheTransaction.getGlobalTransaction();
      }

      @Override
      public synchronized void awaitUntilCommitted() throws InterruptedException {
         while (!committed) {
            wait();
         }
      }

      public synchronized void committed() {
         if (log.isTraceEnabled()) {
            log.tracef("Mark transaction committed: %s", this);
         }
         committed = true;
         notifyAll();
      }

      @Override
      public void awaitUntilIsReadyToCommit() throws InterruptedException {
         readyToCommit.await();
      }

      @Override
      public boolean isReadyToCommit() {
         return readyToCommit.getCount() == 0;
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
               ", ready=" + hasReceiveCommitCommand() +
               ", readyToCommit=" + isReadyToCommit() +
               ", committed=" + isCommitted() +
               ", gtx=" + cacheTransaction.getGlobalTransaction().globalId() +
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

   private abstract class AbstractBoundaryNode implements Node {

      @Override
      public void commitVersion(GMUVersion commitCommand) {/*no-op*/}

      @Override
      public GMUVersion getVersion() {
         throw new UnsupportedOperationException();
      }

      @Override
      public boolean hasReceiveCommitCommand() {
         return false;
      }

      @Override
      public void markReadyToCommit() {/*no-op*/}

      @Override
      public GlobalTransaction getGlobalTransaction() {
         throw new UnsupportedOperationException();
      }

      @Override
      public Node getPrevious() {
         throw new UnsupportedOperationException();
      }

      @Override
      public void setPrevious(Node previous) {
         throw new UnsupportedOperationException();
      }

      @Override
      public Node getNext() {
         throw new UnsupportedOperationException();
      }

      @Override
      public void setNext(Node next) {
         throw new UnsupportedOperationException();
      }

      @Override
      public void awaitUntilIsReadyToCommit() throws InterruptedException {/*no-op*/}

      @Override
      public boolean isReadyToCommit() {
         throw new UnsupportedOperationException();
      }

      @Override
      public CacheTransaction getCacheTransactionForCommit() {
         throw new UnsupportedOperationException();
      }

      @Override
      public void committed() {/*no-op*/}

      @Override
      public boolean isCommitted() {
         throw new UnsupportedOperationException();
      }

      @Override
      public void awaitUntilCommitted() throws InterruptedException {/*no-op*/}

      @Override
      public long getConcurrentClockNumber() {
         throw new UnsupportedOperationException();
      }
   }
}
