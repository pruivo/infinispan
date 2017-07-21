package org.infinispan.client.hotrod.impl.transaction;

import static org.infinispan.commons.util.Util.toStr;

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
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.util.ByRef;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
class TransactionContext<K, V> {

   private static final Log log = LogFactory.getLog(TransactionContext.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();

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

   @Override
   public String toString() {
      return "TransactionContext{" +
            "cacheName='" + cacheName + '\'' +
            ", context-size=" + entries.size() + " (entries)" +
            '}';
   }

   <T> T compute(K key, Function<TransactionEntry<K, V>, T> function) {
      ByRef<T> result = new ByRef<>(null);
      entries.compute(wrap(key), (wKey, entry) -> {
         if (entry == null) {
            entry = TransactionEntry.notReadEntry(wKey.key);
         }
         if (trace) {
            log.tracef("Compute key (%s). Before=%s", wKey, entry);
         }
         result.set(function.apply(entry));
         if (trace) {
            log.tracef("Compute key (%s). After=%s (result=%s)", wKey, entry, result.get());
         }
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

   <T> T compute(K key, Function<TransactionEntry<K, V>, T> function,
         Function<K, MetadataValue<V>> remoteValueSupplier) {
      ByRef<T> result = new ByRef<>(null);
      entries.compute(wrap(key), (wKey, entry) -> {
         if (entry == null) {
            entry = createEntryFromRemote(wKey.key, remoteValueSupplier);
            if (trace) {
               log.tracef("Fetched key (%s) from remote. Entry=%s", wKey, entry);
            }
         }
         if (trace) {
            log.tracef("Compute key (%s). Before=%s", wKey, entry);
         }
         result.set(function.apply(entry));
         if (trace) {
            log.tracef("Compute key (%s). After=%s (result=%s)", wKey, entry, result.get());
         }
         return entry;
      });
      return result.get();
   }

   private WrappedKey<K> wrap(K key) {
      return new WrappedKey<>(key);
   }

   private static class WrappedKey<K> {
      private final K key;

      private WrappedKey(K key) {
         this.key = key;
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

         return Objects.deepEquals(key, that.key);
      }

      @Override
      public int hashCode() {
         if (key instanceof Object[]) {
            return Arrays.deepHashCode((Object[]) key);
         } else if (key instanceof byte[]) {
            return Arrays.hashCode((byte[]) key);
         } else if (key instanceof short[]) {
            return Arrays.hashCode((short[]) key);
         } else if (key instanceof int[]) {
            return Arrays.hashCode((int[]) key);
         } else if (key instanceof long[]) {
            return Arrays.hashCode((long[]) key);
         } else if (key instanceof char[]) {
            return Arrays.hashCode((char[]) key);
         } else if (key instanceof float[]) {
            return Arrays.hashCode((float[]) key);
         } else if (key instanceof double[]) {
            return Arrays.hashCode((double[]) key);
         } else if (key instanceof boolean[]) {
            return Arrays.hashCode((boolean[]) key);
         } else {
            return Objects.hashCode(key);
         }
      }

      @Override
      public String toString() {
         return "WrappedKey{" +
               "key=" + toStr(key) +
               '}';
      }
   }

}
