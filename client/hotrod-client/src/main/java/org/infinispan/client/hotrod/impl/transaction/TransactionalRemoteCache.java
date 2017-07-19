package org.infinispan.client.hotrod.impl.transaction;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.VersionedValue;
import org.infinispan.client.hotrod.impl.RemoteCacheImpl;
import org.infinispan.client.hotrod.impl.transaction.entry.TransactionEntry;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public class TransactionalRemoteCache<K, V> extends RemoteCacheImpl<K, V> {

   private final boolean forceReturnValue;
   private final TransactionManager transactionManager;
   private final TransactionTable transactionTable;

   public TransactionalRemoteCache(RemoteCacheManager rcm, String name, boolean forceReturnValue,
         TransactionManager transactionManager,
         TransactionTable transactionTable) {
      super(rcm, name);
      this.forceReturnValue = forceReturnValue;
      this.transactionManager = transactionManager;
      this.transactionTable = transactionTable;
   }

   @Override
   public boolean removeWithVersion(K key, long version) {
      TransactionContext<K, V> txContext = getTransactionContext();
      return txContext == null ?
            super.removeWithVersion(key, version) :
            txContext.compute(key,
                  entry -> removeEntryIfSameVersion(entry, version),
                  super::getWithMetadata);
   }

   @Override
   public boolean replaceWithVersion(K key, V newValue, long version, long lifespan, TimeUnit lifespanTimeUnit,
         long maxIdle, TimeUnit maxIdleTimeUnit) {
      TransactionContext<K, V> txContext = getTransactionContext();
      return txContext == null ?
            super.replaceWithVersion(key, newValue, version, lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit) :
            txContext.compute(key,
                  entry -> replaceEntryIfSameVersion(entry, newValue, version, lifespan, lifespanTimeUnit, maxIdle,
                        maxIdleTimeUnit),
                  super::getWithMetadata);
   }

   @Override
   public VersionedValue<V> getVersioned(K key) {
      TransactionContext<K, V> txContext = getTransactionContext();
      return txContext == null ?
            super.getVersioned(key) :
            txContext.compute(key, TransactionEntry::toVersionValue, super::getWithMetadata);
   }

   @Override
   public MetadataValue<V> getWithMetadata(K key) {
      TransactionContext<K, V> txContext = getTransactionContext();
      return txContext == null ?
            super.getWithMetadata(key) :
            txContext.compute(key, TransactionEntry::toMetadataValue, super::getWithMetadata);
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit lifespanUnit, long maxIdleTime,
         TimeUnit maxIdleTimeUnit) {
      TransactionContext<K, V> txContext = getTransactionContext();
      if (txContext == null) {
         super.putAll(map, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
      } else {
         map.forEach((key, value) ->
               txContext.compute(key,
                     entry -> getAndSetEntry(entry, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit)));
      }
   }

   @Override
   public V put(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      TransactionContext<K, V> txContext = getTransactionContext();
      if (txContext == null) {
         return super.put(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
      }
      if (forceReturnValue) {
         return txContext.compute(key,
               entry -> getAndSetEntry(entry, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit),
               super::getWithMetadata);
      } else {
         return txContext.compute(key,
               entry -> getAndSetEntry(entry, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit));
      }
   }

   @Override
   public V putIfAbsent(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime,
         TimeUnit maxIdleTimeUnit) {
      TransactionContext<K, V> txContext = getTransactionContext();
      return txContext == null ?
            super.putIfAbsent(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit) :
            txContext.compute(key,
                  entry -> putEntryIfAbsent(entry, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit),
                  super::getWithMetadata);
   }

   @Override
   public V replace(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      TransactionContext<K, V> txContext = getTransactionContext();
      return txContext == null ?
            super.replace(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit) :
            txContext.compute(key,
                  entry -> replaceEntry(entry, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit),
                  super::getWithMetadata);
   }

   @Override
   public boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime,
         TimeUnit maxIdleTimeUnit) {
      TransactionContext<K, V> txContext = getTransactionContext();
      return txContext == null ?
            super.replace(key, oldValue, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit) :
            txContext.compute(key,
                  entry -> replaceEntryIfEquals(entry, oldValue, value, lifespan, lifespanUnit, maxIdleTime,
                        maxIdleTimeUnit),
                  super::getWithMetadata);
   }

   @Override
   public boolean containsKey(Object key) {
      TransactionContext<K, V> txContext = getTransactionContext();
      return (txContext != null && txContext.containsKey(key, super::getWithMetadata)) ||
            super.containsKey(key);
   }

   @Override
   public boolean containsValue(Object value) {
      TransactionContext<K, V> txContext = getTransactionContext();
      return (txContext != null && txContext.containsValue(value)) ||
            super.containsValue(value);
   }

   @Override
   public V get(Object key) {
      TransactionContext<K, V> txContext = getTransactionContext();
      //noinspection unchecked
      return txContext == null ?
            super.get(key) :
            txContext.compute((K) key, TransactionEntry::getValue, super::getWithMetadata);
   }

   @Override
   public V remove(Object key) {
      TransactionContext<K, V> txContext = getTransactionContext();
      if (txContext == null) {
         return super.remove(key);
      }
      if (forceReturnValue) {
         //noinspection unchecked
         return txContext.compute((K) key, this::removeEntry, super::getWithMetadata);
      } else {
         //noinspection unchecked
         return txContext.compute((K) key, this::removeEntry);
      }
   }

   @Override
   public boolean remove(Object key, Object value) {
      TransactionContext<K, V> txContext = getTransactionContext();
      //noinspection unchecked
      return txContext == null ?
            super.remove(key, value) :
            txContext.compute((K) key, entry -> removeEntryIfEquals(entry, value), super::getWithMetadata);
   }

   public TransactionManager getTransactionManager() {
      return transactionManager;
   }

   Function<K, byte[]> keyMarshaller() {
      return k -> super.obj2bytes(k, true);
   }

   Function<V, byte[]> valueMarshaller() {
      return v -> obj2bytes(v, false);
   }

   private boolean removeEntryIfSameVersion(TransactionEntry<K, V> entry, long version) {
      if (entry.exists() && entry.getVersion() == version) {
         entry.remove();
         return true;
      } else {
         return false;
      }
   }

   private boolean replaceEntryIfSameVersion(TransactionEntry<K, V> entry, V newValue, long version, long lifespan,
         TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit) {
      if (entry.exists() && entry.getVersion() == version) {
         entry.set(newValue, lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit);
         return true;
      } else {
         return false;
      }
   }

   private V putEntryIfAbsent(TransactionEntry<K, V> entry, V value, long lifespan, TimeUnit lifespanTimeUnit,
         long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      V currentValue = entry.getValue();
      if (currentValue == null) {
         entry.set(value, lifespan, lifespanTimeUnit, maxIdleTime, maxIdleTimeUnit);
      }
      return currentValue;
   }

   private V replaceEntry(TransactionEntry<K, V> entry, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime,
         TimeUnit maxIdleTimeUnit) {
      V currentValue = entry.getValue();
      if (currentValue != null) {
         entry.set(value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
      }
      return currentValue;
   }

   private boolean replaceEntryIfEquals(TransactionEntry<K, V> entry, V oldValue, V value, long lifespan,
         TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      V currentValue = entry.getValue();
      if (currentValue != null && currentValue.equals(oldValue)) {
         entry.set(value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
         return true;
      } else {
         return false;
      }
   }

   private V removeEntry(TransactionEntry<K, V> entry) {
      V oldValue = entry.getValue();
      entry.remove();
      return oldValue;
   }

   private boolean removeEntryIfEquals(TransactionEntry<K, V> entry, Object value) {
      V oldValue = entry.getValue();
      if (oldValue != null && oldValue.equals(value)) {
         entry.remove();
         return true;
      }
      return false;
   }

   private V getAndSetEntry(TransactionEntry<K, V> entry, V value, long lifespan, TimeUnit lifespanUnit,
         long maxIdleTime,
         TimeUnit maxIdleTimeUnit) {
      V oldValue = entry.getValue();
      entry.set(value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
      return oldValue;
   }

   private TransactionContext<K, V> getTransactionContext() {
      assertRemoteCacheManagerIsStarted();
      Transaction tx = getRunningTransaction();
      return tx == null ? null : transactionTable.enlist(this, tx);
   }

   private Transaction getRunningTransaction() {
      try {
         return transactionManager.getTransaction();
      } catch (SystemException e) {
         return null;
      }
   }
}
