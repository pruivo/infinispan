package org.infinispan.container.entries.gmu;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.container.entries.metadata.MetadataAware;
import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.metadata.Metadata;
import org.infinispan.util.Util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
public class InternalGMUNullCacheValue implements InternalCacheValue, Cloneable, MetadataAware {

   private Metadata metadata;

   public InternalGMUNullCacheValue(Metadata metadata) {
      this.metadata = metadata;
   }

   @Override
   public Object getValue() {
      return null;
   }

   @Override
   public InternalCacheEntry toInternalCacheEntry(Object key) {
      return new InternalGMUNullCacheEntry(key, metadata);
   }

   @Override
   public boolean isExpired(long now) {
      return false;
   }

   @Override
   public boolean isExpired() {
      return false;
   }

   @Override
   public boolean canExpire() {
      return false;
   }

   @Override
   public long getCreated() {
      return -1;
   }

   @Override
   public long getLastUsed() {
      return -1;
   }

   @Override
   public long getLifespan() {
      return -1;
   }

   @Override
   public long getMaxIdle() {
      return -1;
   }

   @Override
   public String toString() {
      return "InternalGMUNullCacheValue{" +
            "metadata=" + metadata +
            "}";
   }

   @Override
   public Metadata getMetadata() {
      return metadata;
   }

   @Override
   public void setMetadata(Metadata metadata) {
      this.metadata = metadata;
   }

   @Override
   public InternalGMUNullCacheValue clone() throws CloneNotSupportedException {
      InternalGMUNullCacheValue value = (InternalGMUNullCacheValue) super.clone();
      value.setMetadata(metadata);
      return value;
   }

   public static class Externalizer extends AbstractExternalizer<InternalGMUNullCacheValue> {

      @Override
      public Set<Class<? extends InternalGMUNullCacheValue>> getTypeClasses() {
         return Util.<Class<? extends InternalGMUNullCacheValue>>asSet(InternalGMUNullCacheValue.class);
      }

      @Override
      public void writeObject(ObjectOutput output, InternalGMUNullCacheValue object) throws IOException {
         output.writeObject(object.metadata);
      }

      @Override
      public InternalGMUNullCacheValue readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Metadata metadata = (Metadata) input.readObject();
         return new InternalGMUNullCacheValue(metadata);
      }

      @Override
      public Integer getId() {
         return Ids.INTERNAL_GMU_NULL_VALUE;
      }
   }
}
