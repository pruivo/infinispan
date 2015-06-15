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

   public long checkAndAwaitPendingTxForKey(TxInvocationContext<?> ctx, Object key, long lockTimeout) throws InterruptedException {
      final CacheTransaction tx = ctx.getCacheTransaction();
      boolean isFromStateTransfer = ctx.isOriginLocal() && ((LocalTransaction) tx).isFromStateTransfer();
      // if the transaction is from state transfer it should not wait for the backup locks of other transactions
      if (!isFromStateTransfer) {
         final int transactionTopologyId = tx.getTopologyId();
         if (transactionTopologyId != TransactionTable.CACHE_STOPPED_TOPOLOGY_ID) {
            if (transactionTable.getMinTopologyId() < transactionTopologyId) {
               return checkForPendingLocks(key, ctx.getGlobalTransaction(), transactionTopologyId, lockTimeout);
            }
         }
      }

      if (trace)
         log.tracef("Locking key %s, no need to check for pending locks.", toStr(key));
      return lockTimeout;
   }

   private long checkForPendingLocks(Object key, GlobalTransaction globalTransaction, int transactionTopologyId, long lockTimeout) throws InterruptedException {
      if (trace)
         log.tracef("Checking for pending locks and then locking key %s", toStr(key));

      final long expectedEndTime = timeService.expectedEndTime(lockTimeout, TimeUnit.MILLISECONDS);

      waitForTransactionsToComplete(globalTransaction, getAndFilterTransactions(transactionTopologyId, globalTransaction), key, expectedEndTime);

      // Then try to acquire a lock
      if (trace)
         log.tracef("Finished waiting for other potential lockers, trying to acquire the lock on %s", toStr(key));

      return timeService.remainingTime(expectedEndTime, TimeUnit.MILLISECONDS);
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

   private Collection<CacheTransaction> getAndFilterTransactions(int transactionTopologyId,
                                                                 GlobalTransaction globalTransaction) {
      Collection<? extends CacheTransaction> localTransactions = transactionTable.getLocalTransactions();
      Collection<? extends CacheTransaction> remoteTransactions = transactionTable.getRemoteTransactions();
      List<CacheTransaction> allTransactions = new ArrayList<>(localTransactions.size() + remoteTransactions.size());

      filterAndAdd(localTransactions, transactionTopologyId, globalTransaction, allTransactions);
      filterAndAdd(remoteTransactions, transactionTopologyId, globalTransaction, allTransactions);

      return allTransactions.isEmpty() ? Collections.emptyList() : allTransactions;
   }

   private void filterAndAdd(Collection<? extends CacheTransaction> toFilter, int transactionTopologyId,
                             GlobalTransaction thisGlobalTransaction, Collection<CacheTransaction> toAdd) {
      if (toFilter.isEmpty()) {
         return;
      }
      for (CacheTransaction transaction : toFilter) {
         if (transaction.getTopologyId() < transactionTopologyId) {
            // don't wait for the current transaction
            if (transaction.getGlobalTransaction().equals(thisGlobalTransaction)) {
               continue;
            }
            toAdd.add(transaction);
         }
      }
   }
}
