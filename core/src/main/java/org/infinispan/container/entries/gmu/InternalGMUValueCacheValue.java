package org.infinispan.container.entries.gmu;

import org.infinispan.container.entries.InternalCacheEntry;
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
public class InternalGMUValueCacheValue implements InternalGMUCacheValue {

   private final InternalCacheValue internalCacheValue;
   private final EntryVersion creationVersion;
   private final EntryVersion maxTxVersion;
   private final EntryVersion maxValidVersion;
   private final boolean mostRecent;

   public InternalGMUValueCacheValue(InternalCacheValue internalCacheValue, EntryVersion maxTxVersion,
                                     boolean mostRecent, EntryVersion creationVersion, EntryVersion maxValidVersion) {
      this.internalCacheValue = internalCacheValue;
      this.creationVersion = creationVersion;
      this.maxTxVersion = maxTxVersion;
      this.maxValidVersion = maxValidVersion;
      this.mostRecent = mostRecent;
   }

   @Override
   public Object getValue() {
      return internalCacheValue.getValue();
   }

   @Override
   public InternalCacheEntry toInternalCacheEntry(Object key) {
      return new InternalGMUValueCacheEntry(internalCacheValue.toInternalCacheEntry(key), maxTxVersion, mostRecent,
                                            creationVersion, maxValidVersion);
   }

   @Override
   public boolean isExpired(long now) {
      return internalCacheValue.isExpired(now);
   }

   @Override
   public boolean isExpired() {
      return internalCacheValue.isExpired();
   }

   @Override
   public boolean canExpire() {
      return internalCacheValue.canExpire();
   }

   @Override
   public long getCreated() {
      return internalCacheValue.getCreated();
   }

   @Override
   public long getLastUsed() {
      return internalCacheValue.getLastUsed();
   }

   @Override
   public long getLifespan() {
      return internalCacheValue.getLifespan();
   }

   @Override
   public long getMaxIdle() {
      return internalCacheValue.getMaxIdle();
   }

   @Override
   public EntryVersion getCreationVersion() {
      return creationVersion;
   }

   @Override
   public EntryVersion getMaximumValidVersion() {
      return maxValidVersion;
   }

   @Override
   public EntryVersion getMaximumTransactionVersion() {
      return maxTxVersion;
   }

   @Override
   public boolean isMostRecent() {
      return mostRecent;
   }

   @Override
   public InternalCacheValue getInternalCacheValue() {
      return internalCacheValue;
   }

   @Override
   public String toString() {
      return "InternalGMUValueCacheValue{" +
            "internalCacheValue=" + internalCacheValue +
            ", creationVersion=" + creationVersion +
            ", maxTxVersion=" + maxTxVersion +
            ", maxValidVersion=" + maxValidVersion +
            ", mostRecent=" + mostRecent +
            '}';
   }

   public static class Externalizer extends AbstractExternalizer<InternalGMUValueCacheValue> {

      @Override
      public Set<Class<? extends InternalGMUValueCacheValue>> getTypeClasses() {
         return Util.<Class<? extends InternalGMUValueCacheValue>>asSet(InternalGMUValueCacheValue.class);
      }

      @Override
      public void writeObject(ObjectOutput output, InternalGMUValueCacheValue object) throws IOException {
         output.writeObject(object.internalCacheValue);
         output.writeObject(object.creationVersion);
         output.writeObject(object.maxTxVersion);
         output.writeObject(object.maxValidVersion);
         output.writeBoolean(object.mostRecent);
      }

      @Override
      public InternalGMUValueCacheValue readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         InternalCacheValue internalCacheValue = (InternalCacheValue) input.readObject();
         EntryVersion creationVersion = (EntryVersion) input.readObject();
         EntryVersion maxTxVersion = (EntryVersion) input.readObject();
         EntryVersion maxValidVersion = (EntryVersion) input.readObject();
         boolean mostRecent = input.readBoolean();
         return new InternalGMUValueCacheValue(internalCacheValue, maxTxVersion, mostRecent, creationVersion,
                                               maxValidVersion
         );
      }

      @Override
      public Integer getId() {
         return Ids.INTERNAL_GMU_VALUE;
      }
   }
}
