package org.infinispan.container.entries.gmu;

import com.sun.tools.javac.resources.version;
import org.infinispan.container.entries.AbstractInternalCacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.container.entries.metadata.MetadataAware;
import org.infinispan.container.versioning.EntryVersion;
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
 * @since 5.2
 */
public class InternalGMUNullCacheEntry extends AbstractInternalCacheEntry {

   private Metadata metadata;

   public InternalGMUNullCacheEntry(InternalCacheEntry expired) {
      this(expired.getKey(), expired.getMetadata());
   }

   public InternalGMUNullCacheEntry(Object key, Metadata metadata) {
      super(key);
      this.metadata = metadata;
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
   public long getExpiryTime() {
      return -1;
   }

   @Override
   public void touch() {}

   @Override
   public void touch(long currentTimeMillis) {}

   @Override
   public void reincarnate() {}

   @Override
   public void reincarnate(long now) {}

   @Override
   public InternalCacheValue toInternalCacheValue() {
      return new InternalGMUNullCacheValue(metadata);
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
   public boolean isNull() {
      return true;
   }

   @Override
   public String toString() {
      return "InternalGMUNullCacheEntry{" +
            "key=" + getKey() +
            ", metadata=" + metadata +
            "}";
   }

   @Override
   public Object getValue() {
      return null;
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
   public Object setValue(Object value) {
      return null;
   }



   @Override
   public InternalGMUNullCacheEntry clone() {
      InternalGMUNullCacheEntry entry = (InternalGMUNullCacheEntry) super.clone();
      //metadata is immutable!
      entry.setMetadata(metadata);
      return entry;
   }

   public static class Externalizer extends AbstractExternalizer<InternalGMUNullCacheEntry> {

      @Override
      public Set<Class<? extends InternalGMUNullCacheEntry>> getTypeClasses() {
         return Util.<Class<? extends InternalGMUNullCacheEntry>>asSet(InternalGMUNullCacheEntry.class);
      }

      @Override
      public void writeObject(ObjectOutput output, InternalGMUNullCacheEntry object) throws IOException {
         output.writeObject(object.getKey());
         output.writeObject(object.getMetadata());
      }

      @Override
      public InternalGMUNullCacheEntry readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Object key = input.readObject();
         Metadata metadata = (Metadata) input.readObject();
         return new InternalGMUNullCacheEntry(key, metadata);
      }

      @Override
      public Integer getId() {
         return Ids.INTERNAL_GMU_NULL_ENTRY;
      }
   }
}
