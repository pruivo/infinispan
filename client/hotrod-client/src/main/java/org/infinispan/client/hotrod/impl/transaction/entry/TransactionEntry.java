package org.infinispan.client.hotrod.impl.transaction.entry;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.VersionedValue;
import org.infinispan.client.hotrod.impl.MetadataValueImpl;
import org.infinispan.client.hotrod.impl.VersionedValueImpl;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public class TransactionEntry<K, V> {
   private final K key;
   private final long version; //version read. never changes during the transaction
   private final byte readControl;
   private V value; //null == removed
   private long created = -1;
   private long lifespan = -1;
   private TimeUnit lifespanTimeUnit;
   private long maxIdle = -1;
   private TimeUnit maxIdleTimeUnit;
   private boolean modified;

   private TransactionEntry(K key, long version, byte readControl) {
      this.key = key;
      this.version = version;
      this.readControl = readControl;
      this.modified = false;
   }

   public static <K, V> TransactionEntry<K, V> nonExistingEntry(K key) {
      return new TransactionEntry<>(key, 0, ControlByte.NON_EXISTING.bit());
   }

   public static <K, V> TransactionEntry<K, V> notReadEntry(K key) {
      return new TransactionEntry<>(key, 0, ControlByte.NOT_READ.bit());
   }

   public static <K, V> TransactionEntry<K, V> read(K key, MetadataValue<V> value) {
      TransactionEntry<K, V> entry = new TransactionEntry<>(key, value.getVersion(), (byte) 0);
      entry.init(value);
      return entry;
   }

   public long getVersion() {
      return version;
   }

   public V getValue() {
      return value;
   }

   public VersionedValue<V> toVersionValue() {
      return isNonExists() ?
            null :
            new VersionedValueImpl<>(version, value);
   }

   public MetadataValue<V> toMetadataValue() {
      return isNonExists() ?
            null :
            new MetadataValueImpl<>(created, getLifespan(), System.currentTimeMillis(), getMaxIdle(), version, value);
   }

   public boolean isModified() {
      return modified;
   }

   public boolean isNonExists() {
      return value == null;
   }

   public boolean exists() {
      return value != null;
   }

   public void set(V value, long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit) {
      final boolean created = this.value == null;
      this.value = value;
      this.lifespan = lifespan;
      this.maxIdle = maxIdle;
      this.lifespanTimeUnit = lifespanTimeUnit;
      this.maxIdleTimeUnit = maxIdleTimeUnit;
      if (created && lifespan >= 0) {
         this.created = System.currentTimeMillis();
      }
   }

   private int getLifespan() {
      return lifespan < 0 ? -1 : (int) lifespanTimeUnit.toSeconds(lifespan);
   }

   private int getMaxIdle() {
      return maxIdle < 0 ? -1 : (int) maxIdleTimeUnit.toSeconds(maxIdle);
   }

   public void remove() {
      this.value = null;
   }

   public Modification toModification(Function<K, byte[]> keyMarshaller, Function<V, byte[]> valueMarshaller) {
      if (value == null) {
         //remove operation
         return new Modification(keyMarshaller.apply(key), null, version, 0, 0, null, null,
               ControlByte.REMOVE_OP.set(readControl));
      } else {
         return new Modification(keyMarshaller.apply(key), valueMarshaller.apply(value), version, lifespan, maxIdle,
               lifespanTimeUnit, maxIdleTimeUnit, readControl);
      }
   }

   private void init(MetadataValue<V> metadataValue) {
      this.value = metadataValue.getValue();
      this.created = metadataValue.getCreated();
      this.lifespan = metadataValue.getLifespan();
      this.lifespanTimeUnit = TimeUnit.SECONDS;
      this.maxIdle = metadataValue.getMaxIdle();
      this.maxIdleTimeUnit = TimeUnit.SECONDS;
   }
}
