package org.infinispan.container.entries.gmu;

import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
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
public class InternalGMUNullCacheEntry extends InternalGMURemovedCacheEntry {

   private final boolean mostRecent;
   private EntryVersion creationVersion;
   private EntryVersion maxValidVersion;
   private final EntryVersion maxTxVersion;

   public InternalGMUNullCacheEntry(Object key, EntryVersion version, EntryVersion maxTxVersion, boolean mostRecent,
                                    EntryVersion creationVersion, EntryVersion maxValidVersion) {
      super(key, version);
      this.creationVersion = creationVersion;
      this.maxValidVersion = maxValidVersion;
      this.maxTxVersion = maxTxVersion;
      this.mostRecent = mostRecent;
   }

   public InternalGMUNullCacheEntry(InternalGMUCacheEntry expired) {
      super(expired.getKey(), expired.getVersion());
      this.creationVersion = expired.getCreationVersion();
      this.maxValidVersion = expired.getMaximumValidVersion();
      this.maxTxVersion = expired.getMaximumTransactionVersion();
      this.mostRecent = expired.isMostRecent();
   }

   @Override
   public InternalCacheValue toInternalCacheValue() {
      return new InternalGMUNullCacheValue(getVersion(), maxTxVersion, mostRecent, creationVersion, maxValidVersion);
   }

   @Override
   public EntryVersion getMaximumTransactionVersion() {
      return maxTxVersion;
   }

   @Override
   public EntryVersion getMaximumValidVersion() {
      return maxValidVersion;
   }

   @Override
   public EntryVersion getCreationVersion() {
      return creationVersion;
   }

   @Override
   public boolean isMostRecent() {
      return mostRecent;
   }

   @Override
   public void setCreationVersion(EntryVersion entryVersion) {
      this.creationVersion = entryVersion;
   }

   @Override
   public void setMaximumValidVersion(EntryVersion version) {
      this.maxValidVersion = version;
   }

   @Override
   public String toString() {
      return "InternalGMUNullCacheEntry{" +
            "key=" + getKey() +
            ", version=" + getVersion() +
            ", value=" + getValue() +
            ", creationVersion=" + creationVersion +
            ", maxValidVersion=" + maxValidVersion +
            ", maxTxVersion=" + maxTxVersion +
            ", mostRecent=" + mostRecent +
            "}";
   }

   public static class Externalizer extends AbstractExternalizer<InternalGMUNullCacheEntry> {

      @Override
      public Set<Class<? extends InternalGMUNullCacheEntry>> getTypeClasses() {
         return Util.<Class<? extends InternalGMUNullCacheEntry>>asSet(InternalGMUNullCacheEntry.class);
      }

      @Override
      public void writeObject(ObjectOutput output, InternalGMUNullCacheEntry object) throws IOException {
         output.writeObject(object.getKey());
         output.writeObject(object.getVersion());
         output.writeObject(object.creationVersion);
         output.writeObject(object.maxTxVersion);
         output.writeObject(object.maxValidVersion);
         output.writeBoolean(object.mostRecent);
      }

      @Override
      public InternalGMUNullCacheEntry readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Object key = input.readObject();
         EntryVersion version = (EntryVersion) input.readObject();
         EntryVersion creationVersion = (EntryVersion) input.readObject();
         EntryVersion maxTxVersion = (EntryVersion) input.readObject();
         EntryVersion maxValidVersion = (EntryVersion) input.readObject();
         boolean mostRecent = input.readBoolean();
         return new InternalGMUNullCacheEntry(key, version, maxTxVersion, mostRecent, creationVersion, maxValidVersion);
      }

      @Override
      public Integer getId() {
         return Ids.INTERNAL_GMU_NULL_ENTRY;
      }
   }
}
