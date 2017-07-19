package org.infinispan.client.hotrod.impl.transaction;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.impl.operations.OperationsFactory;
import org.infinispan.client.hotrod.impl.transaction.entry.Modification;
import org.infinispan.client.hotrod.impl.transaction.entry.TransactionEntry;
import org.infinispan.commons.util.ByRef;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
class TransactionContext<K, V> {

   private final Map<WrappedKey<K>, TransactionEntry<K, V>> entries;
   private final Function<K, byte[]> keyMarshaller;
   private final Function<V, byte[]> valueMarshaller;
   private final OperationsFactory operationsFactory;
   private final String cacheName;

   TransactionContext(Function<K, byte[]> keyMarshaller, Function<V, byte[]> valueMarshaller,
         OperationsFactory operationsFactory, String cacheName) {
      this.keyMarshaller = keyMarshaller;
      this.valueMarshaller = valueMarshaller;
      this.operationsFactory = operationsFactory;
      this.cacheName = cacheName;
      entries = new ConcurrentHashMap<>();
   }

   boolean containsKey(Object key, Function<K, MetadataValue<V>> remoteValueSupplier) {
      ByRef<Boolean> result = new ByRef<>(null);
      //noinspection unchecked
      entries.compute(wrap((K) key), (wKey, entry) -> {
         if (entry == null) {
            entry = createEntryFromRemote(wKey.key, remoteValueSupplier);
         }
         result.set(!entry.isNonExists());
         return entry;
      });
      return result.get();
   }

   boolean containsValue(Object value) {
      return entries.values().stream()
            .map(TransactionEntry::getValue)
            .filter(Objects::nonNull)
            .anyMatch(v -> v.equals(value));
   }

   OperationsFactory getOperationsFactory() {
      return operationsFactory;
   }

   String getCacheName() {
      return cacheName;
   }

   <T> T compute(K key, Function<TransactionEntry<K, V>, T> function,
         Function<K, MetadataValue<V>> remoteValueSupplier) {
      ByRef<T> result = new ByRef<>(null);
      entries.compute(wrap(key), (wKey, entry) -> {
         if (entry == null) {
            entry = createEntryFromRemote(wKey.key, remoteValueSupplier);
         }
         result.set(function.apply(entry));
         return entry;
      });
      return result.get();
   }

   <T> T compute(K key, Function<TransactionEntry<K, V>, T> function) {
      ByRef<T> result = new ByRef<>(null);
      entries.compute(wrap(key), (wKey, entry) -> {
         if (entry == null) {
            entry = TransactionEntry.notReadEntry(wKey.key);
         }
         result.set(function.apply(entry));
         return entry;
      });
      return result.get();
   }

   private TransactionEntry<K, V> createEntryFromRemote(K key, Function<K, MetadataValue<V>> remoteValueSupplier) {
      MetadataValue<V> remoteValue = remoteValueSupplier.apply(key);
      return remoteValue == null ? TransactionEntry.nonExistingEntry(key) : TransactionEntry.read(key, remoteValue);
   }

   Collection<Modification> toModification() {
      return entries.values().stream()
            .filter(TransactionEntry::isModified)
            .map(entry -> entry.toModification(keyMarshaller, valueMarshaller))
            .collect(Collectors.toList());
   }

   private WrappedKey<K> wrap(K key) {
      return new WrappedKey<>(key);
   }

   private static class WrappedKey<K> {
      private final K key;
      private final boolean isArray;

      private WrappedKey(K key) {
         this.key = key;
         isArray = key.getClass().isArray();
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) {
            return true;
         }
         if (o == null || getClass() != o.getClass()) {
            return false;
         }

         WrappedKey<?> that = (WrappedKey<?>) o;

         return isArray && that.isArray && isKeyEquals(that);
      }

      @Override
      public int hashCode() {
         return isArray ? Arrays.hashCode(keyAsArray()) : key.hashCode();
      }

      private boolean isKeyEquals(WrappedKey<?> other) {
         return isArray ? Arrays.equals(keyAsArray(), other.keyAsArray()) : key.equals(other.key);
      }

      private Object[] keyAsArray() {
         return (Object[]) key;
      }
   }

}
