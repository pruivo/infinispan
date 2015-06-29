package org.infinispan.util.concurrent.locks.impl;

import org.infinispan.commons.util.Notifier;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.TimeService;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.locks.PendingLockManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.infinispan.commons.util.Util.toStr;
import static org.infinispan.factories.KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public class DefaultPendingLockManager implements PendingLockManager {

   private static final Log log = LogFactory.getLog(DefaultPendingLockManager.class);
   private static final boolean trace = log.isTraceEnabled();
   private static final int NO_PENDING_CHECK = -2;
   private final Map<GlobalTransaction, PendingLockPromiseImpl> pendingLockPromiseMap;
   private TransactionTable transactionTable;
   private TimeService timeService;
   private ScheduledExecutorService timeoutExecutor;

   public DefaultPendingLockManager() {
      pendingLockPromiseMap = new ConcurrentHashMap<>();
   }

   private static <T> T findAnyCommon(Collection<T> c1, Collection<T> c2) {
      if (c1.isEmpty()) {
         return null;
      } else if (c2.isEmpty()) {
         return null;
      }
      for (T item : c1) {
         if (c2.contains(item)) {
            return item;
         }
      }
      return null;
   }

   @Inject
   public void inject(TransactionTable transactionTable, TimeService timeService,
                      @ComponentName(TIMEOUT_SCHEDULE_EXECUTOR) ScheduledExecutorService timeoutExecutor) {
      this.transactionTable = transactionTable;
      this.timeService = timeService;
      this.timeoutExecutor = timeoutExecutor;
   }

   @Override
   public PendingLockPromise checkPendingForKey(TxInvocationContext<?> ctx, Object key, long time, TimeUnit unit) {
      final GlobalTransaction globalTransaction = ctx.getGlobalTransaction();
      PendingLockPromiseImpl existing = pendingLockPromiseMap.get(globalTransaction);
      if (existing != null) {
         return existing;
      }
      final int txTopologyId = getTopologyId(ctx);
      if (txTopologyId != NO_PENDING_CHECK) {
         if (trace) {
            log.tracef("Checking for pending locks and then locking key %s", toStr(key));
         }
         return createAndStore(getTransactionWithLockedKey(txTopologyId, key, globalTransaction),
                               globalTransaction, time, unit);
      }
      return PendingLockPromise.NO_OP;
   }

   @Override
   public PendingLockPromise checkPendingForAllKeys(TxInvocationContext<?> ctx, Collection<Object> keys,
                                                    long time, TimeUnit unit) {
      final GlobalTransaction globalTransaction = ctx.getGlobalTransaction();
      PendingLockPromiseImpl existing = pendingLockPromiseMap.get(globalTransaction);
      if (existing != null) {
         return existing;
      }
      final int txTopologyId = getTopologyId(ctx);
      if (txTopologyId != NO_PENDING_CHECK) {
         if (trace) {
            log.tracef("Checking for pending locks and then locking keys %s", toStr(keys));
         }
         return createAndStore(getTransactionWithAnyLockedKey(txTopologyId, keys, globalTransaction),
                               globalTransaction, time, unit);
      }
      return PendingLockPromise.NO_OP;
   }

   @Override
   public long awaitPendingTransactionsForKey(TxInvocationContext<?> ctx, Object key,
                                              long time, TimeUnit unit) throws InterruptedException {
      PendingLockPromiseImpl pendingLockPromise = pendingLockPromiseMap.remove(ctx.getGlobalTransaction());
      if (pendingLockPromise != null) {
         pendingLockPromise.await();
         if (pendingLockPromise.hasTimedOut()) {
            timeoutForKey(key, pendingLockPromise.getTimedOutTransaction(), ctx.getGlobalTransaction());
         }
         return pendingLockPromise.getRemainingTimeout();
      }
      final int txTopologyId = getTopologyId(ctx);
      if (txTopologyId != NO_PENDING_CHECK) {
         return checkForPendingLock(key, ctx.getGlobalTransaction(), txTopologyId, unit.toMillis(time));
      }

      if (trace) {
         log.tracef("Locking key %s, no need to check for pending locks.", toStr(key));
      }
      return unit.toMillis(time);
   }

   @Override
   public long awaitPendingTransactionsForAllKeys(TxInvocationContext<?> ctx, Collection<Object> keys,
                                                  long time, TimeUnit unit) throws InterruptedException {
      PendingLockPromiseImpl pendingLockPromise = pendingLockPromiseMap.remove(ctx.getGlobalTransaction());
      if (pendingLockPromise != null) {
         pendingLockPromise.await();
         if (pendingLockPromise.hasTimedOut()) {
            timeoutForAnyKey(keys, pendingLockPromise.getTimedOutTransaction(), ctx.getGlobalTransaction());
         }
         return pendingLockPromise.getRemainingTimeout();
      }
      final int txTopologyId = getTopologyId(ctx);
      if (txTopologyId != NO_PENDING_CHECK) {
         return checkForPendingAnyLocks(keys, ctx.getGlobalTransaction(), txTopologyId, unit.toMillis(time));
      }

      if (trace) {
         log.tracef("Locking keys %s, no need to check for pending locks.", toStr(keys));
      }

      return unit.toMillis(time);
   }

   private PendingLockPromise createAndStore(Collection<CacheTransaction> transactions,
                                             GlobalTransaction globalTransaction, long time, TimeUnit unit) {
      if (transactions.isEmpty()) {
         return PendingLockPromise.NO_OP;
      }

      PendingLockPromiseImpl pendingLockPromise = new PendingLockPromiseImpl(transactions, timeService.expectedEndTime(time, unit));
      PendingLockPromiseImpl existing = pendingLockPromiseMap.putIfAbsent(globalTransaction, pendingLockPromise);
      if (existing != null) {
         return existing;
      }
      pendingLockPromise.registerListenerInCacheTransactions();
      if (!pendingLockPromise.isReady()) {
         timeoutExecutor.schedule(pendingLockPromise, time, unit);
      }
      return pendingLockPromise;
   }

   private int getTopologyId(TxInvocationContext<?> context) {
      final CacheTransaction tx = context.getCacheTransaction();
      boolean isFromStateTransfer = context.isOriginLocal() && ((LocalTransaction) tx).isFromStateTransfer();
      // if the transaction is from state transfer it should not wait for the backup locks of other transactions
      if (!isFromStateTransfer) {
         final int transactionTopologyId = tx.getTopologyId();
         if (transactionTopologyId != TransactionTable.CACHE_STOPPED_TOPOLOGY_ID) {
            if (transactionTable.getMinTopologyId() < transactionTopologyId) {
               return transactionTopologyId;
            }
         }
      }
      return NO_PENDING_CHECK;
   }

   private long checkForPendingLock(Object key, GlobalTransaction globalTransaction, int transactionTopologyId, long lockTimeout) throws InterruptedException {
      if (trace) {
         log.tracef("Checking for pending locks and then locking key %s", toStr(key));
      }

      final long expectedEndTime = timeService.expectedEndTime(lockTimeout, TimeUnit.MILLISECONDS);

      final CacheTransaction lockOwner = waitForTransactionsToComplete(getTransactionWithLockedKey(transactionTopologyId, key, globalTransaction),
                                                                       expectedEndTime);

      // Then try to acquire a lock
      if (trace) {
         log.tracef("Finished waiting for other potential lockers, trying to acquire the lock on %s", toStr(key));
      }

      if (lockOwner != null) {
         timeoutForKey(key, lockOwner, globalTransaction);
      }

      return timeService.remainingTime(expectedEndTime, TimeUnit.MILLISECONDS);
   }

   private long checkForPendingAnyLocks(Collection<Object> keys, GlobalTransaction globalTransaction, int transactionTopologyId, long lockTimeout) throws InterruptedException {
      if (trace)
         log.tracef("Checking for pending locks and then locking key %s", toStr(keys));

      final long expectedEndTime = timeService.expectedEndTime(lockTimeout, TimeUnit.MILLISECONDS);

      final CacheTransaction lockOwner = waitForTransactionsToComplete(getTransactionWithAnyLockedKey(transactionTopologyId, keys, globalTransaction),
                                                                       expectedEndTime);

      // Then try to acquire a lock
      if (trace) {
         log.tracef("Finished waiting for other potential lockers. Timed-Out? %b. trying to acquire the lock on %s",
                    lockOwner != null, toStr(keys));
      }

      if (lockOwner != null) {
         timeoutForAnyKey(keys, lockOwner, globalTransaction);
      }

      return timeService.remainingTime(expectedEndTime, TimeUnit.MILLISECONDS);
   }

   private CacheTransaction waitForTransactionsToComplete(Collection<? extends CacheTransaction> transactionsToCheck,
                                                          long expectedEndTime) throws InterruptedException {
      if (transactionsToCheck.isEmpty()) {
         return null;
      }
      for (CacheTransaction tx : transactionsToCheck) {
         long remaining;
         if ((remaining = timeService.remainingTime(expectedEndTime, TimeUnit.MILLISECONDS)) > 0) {
            if (!tx.waitForLockRelease(remaining)) {
               return tx;
            }
         }
      }
      return null;
   }

   private void timeoutForKey(Object key, CacheTransaction tx, GlobalTransaction thisGlobalTransaction) {
      throw new TimeoutException(format("Could not acquire lock on %s in behalf of transaction %s. Current owner %s.",
                                        key, thisGlobalTransaction, tx.getGlobalTransaction()));
   }

   private void timeoutForAnyKey(Collection<Object> keys, CacheTransaction tx, GlobalTransaction thisGlobalTransaction) {
      Object key = findAnyCommon(keys, tx.getLockedKeys());
      if (key == null) {
         key = findAnyCommon(keys, tx.getBackupLockedKeys());
      }
      timeoutForKey(key, tx, thisGlobalTransaction);
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

   private class PendingLockPromiseImpl implements PendingLockPromise, Notifier.Invoker<PendingLockPromise.Listener>, CacheTransaction.TransactionCompletedListener, Runnable {

      private final Collection<CacheTransaction> pendingTransactions;
      private final long expectedEndTime;
      private final Notifier<PendingLockPromise.Listener> notifier;
      private volatile CacheTransaction timedOutTransaction;

      private PendingLockPromiseImpl(Collection<CacheTransaction> pendingTransactions, long expectedEndTime) {
         this.pendingTransactions = pendingTransactions;
         this.expectedEndTime = expectedEndTime;
         this.notifier = new Notifier<>(this);
      }

      public void registerListenerInCacheTransactions() {
         for (CacheTransaction transaction : pendingTransactions) {
            transaction.addListener(this);
         }
      }

      @Override
      public boolean isReady() {
         if (timedOutTransaction != null) {
            return true;
         }
         for (CacheTransaction cacheTransaction : pendingTransactions) {
            if (!cacheTransaction.areLocksReleased()) {
               if (timeService.remainingTime(expectedEndTime, TimeUnit.MILLISECONDS) <= 0) {
                  timedOutTransaction = cacheTransaction;
               }
               return timedOutTransaction != null;
            }
         }
         return true;
      }

      @Override
      public void addListener(Listener listener) {
         notifier.add(listener);
      }

      public void await() throws InterruptedException {
         notifier.await(timeService.remainingTime(expectedEndTime, TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
      }

      public boolean hasTimedOut() {
         return timedOutTransaction != null;
      }

      @Override
      public long getRemainingTimeout() {
         return timeService.remainingTime(expectedEndTime, TimeUnit.MILLISECONDS);
      }

      public CacheTransaction getTimedOutTransaction() {
         return timedOutTransaction;
      }

      @Override
      public void invoke(Listener invoker) {
         invoker.onReady();
      }

      @Override
      public void onCompletion() {
         if (isReady()) {
            notifier.fireListener();
         }
      }

      @Override
      public void run() {
         isReady();
      }
   }
}
