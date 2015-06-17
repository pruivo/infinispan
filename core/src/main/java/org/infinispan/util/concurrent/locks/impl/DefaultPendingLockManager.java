package org.infinispan.util.concurrent.locks.impl;

import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.TimeService;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static org.infinispan.commons.util.Util.toStr;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public class DefaultPendingLockManager {

   private static final Log log = LogFactory.getLog(DefaultPendingLockManager.class);
   private static final boolean trace = log.isTraceEnabled();

   private TransactionTable transactionTable;
   private TimeService timeService;

   @Inject
   public void inject(TransactionTable transactionTable, TimeService timeService) {
      this.transactionTable = transactionTable;
      this.timeService = timeService;
   }

   public PendingLockPromise checkAndAwaitPendingTransactions(TxInvocationContext<?> ctx, Object key, long time, TimeUnit unit) throws InterruptedException {
      final CacheTransaction tx = ctx.getCacheTransaction();
      boolean isFromStateTransfer = ctx.isOriginLocal() && ((LocalTransaction) tx).isFromStateTransfer();
      // if the transaction is from state transfer it should not wait for the backup locks of other transactions
      if (!isFromStateTransfer) {
         final int transactionTopologyId = tx.getTopologyId();
         if (transactionTopologyId != TransactionTable.CACHE_STOPPED_TOPOLOGY_ID) {
            if (transactionTable.getMinTopologyId() < transactionTopologyId) {
               return checkForPendingLocks(key, ctx.getGlobalTransaction(), transactionTopologyId, unit.toMillis(time));
            }
         }
      }

      if (trace) {
         log.tracef("Locking key %s, no need to check for pending locks.", toStr(key));
      }
      return new NoOpPendingLockPromise(unit.toMillis(time));
   }

   private PendingLockPromise checkForPendingLocks(Object key, GlobalTransaction globalTransaction, int transactionTopologyId, long lockTimeout) throws InterruptedException {
      if (trace)
         log.tracef("Checking for pending locks and then locking key %s", toStr(key));

      final long expectedEndTime = timeService.expectedEndTime(lockTimeout, TimeUnit.MILLISECONDS);

      Collection<CacheTransaction> transactions = getTransactionWithLockedKey(transactionTopologyId, key, globalTransaction);
      waitForTransactionsToComplete(globalTransaction, transactions, key, expectedEndTime);

      // Then try to acquire a lock
      if (trace)
         log.tracef("Finished waiting for other potential lockers, trying to acquire the lock on %s", toStr(key));

      return new NoOpPendingLockPromise(timeService.remainingTime(expectedEndTime, TimeUnit.MILLISECONDS));
   }

   private void waitForTransactionsToComplete(GlobalTransaction thisGlobalTransaction, Collection<? extends CacheTransaction> transactionsToCheck,
                                              Object key, long expectedEndTime) throws InterruptedException {
      if (transactionsToCheck.isEmpty()) {
         return;
      }
      for (CacheTransaction tx : transactionsToCheck) {
         boolean txCompleted = false;

         long remaining;
         while ((remaining = timeService.remainingTime(expectedEndTime, TimeUnit.MILLISECONDS)) > 0) {
            if (tx.waitForLockRelease(key, remaining)) {
               txCompleted = true;
               break;
            }
         }

         if (!txCompleted) {
            throw newTimeoutException(key, tx, thisGlobalTransaction);
         }
      }
   }

   private TimeoutException newTimeoutException(Object key, CacheTransaction tx, GlobalTransaction thisGlobalTransaction) {
      return new TimeoutException("Could not acquire lock on " + key + " on behalf of transaction " +
                                        thisGlobalTransaction + ". Waiting to complete tx: " + tx + ".");
   }

   private Collection<CacheTransaction> getTransactionWithLockedKey(int transactionTopologyId,
                                                                    Object key,
                                                                    GlobalTransaction globalTransaction) {

      Predicate<CacheTransaction> filter = transaction -> transaction.getTopologyId() < transactionTopologyId &&
            !transaction.getGlobalTransaction().equals(globalTransaction) &&
            transaction.containsLockOrBackupLock(key);

      return filterAndCollectTransactions(filter);
   }

   private Collection<CacheTransaction> getTransactionWithAnyLockedKey(int transactionTopologyId,
                                                                       Collection<Object> keys,
                                                                       GlobalTransaction globalTransaction) {

      Predicate<CacheTransaction> filter = transaction -> transaction.getTopologyId() < transactionTopologyId &&
            !transaction.getGlobalTransaction().equals(globalTransaction) &&
            transaction.containsAnyLockOrBackupLock(keys);

      return filterAndCollectTransactions(filter);
   }

   private Collection<CacheTransaction> filterAndCollectTransactions(Predicate<CacheTransaction> filter) {
      final Collection<? extends CacheTransaction> localTransactions = transactionTable.getLocalTransactions();
      final Collection<? extends CacheTransaction> remoteTransactions = transactionTable.getRemoteTransactions();
      final int totalSize = localTransactions.size() + remoteTransactions.size();
      if (totalSize == 0) {
         return Collections.emptyList();
      }
      final List<CacheTransaction> allTransactions = new ArrayList<>(totalSize);
      final Collector<CacheTransaction, ?, Collection<CacheTransaction>> collector = Collectors.toCollection(() -> allTransactions);


      if (!localTransactions.isEmpty()) {
         localTransactions.stream().filter(filter).collect(collector);
      }
      if (!remoteTransactions.isEmpty()) {
         remoteTransactions.stream().filter(filter).collect(collector);
      }

      return allTransactions.isEmpty() ? Collections.emptyList() : allTransactions;
   }

   private static class PendingLock {
      private final Collection<CacheTransaction> pendingTransactions;

      private PendingLock(Collection<CacheTransaction> pendingTransactions) {
         this.pendingTransactions = pendingTransactions;
      }

      public void isReady() {
         for (CacheTransaction transaction : pendingTransactions) {
            //TODO check it!
         }
      }


   }

   public static class NoOpPendingLockPromise implements PendingLockPromise {

      private final long timeout;

      public NoOpPendingLockPromise(long timeout) {
         this.timeout = timeout;
      }

      @Override
      public long getRemainingTimeout() {
         return timeout;
      }

      @Override
      public boolean isAvailable() {
         return true;
      }

      @Override
      public void lock() throws InterruptedException, TimeoutException {/*no-op*/}

      @Override
      public void addListener(Listener listener) {
         listener.onEvent(true);
      }
   }
}
