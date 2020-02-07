package org.infinispan.container.entries.metadata;

import static java.lang.Math.min;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.container.entries.ExpiryHelper;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.marshall.core.Ids;
import org.infinispan.metadata.Metadata;

/**
 * A form of {@link org.infinispan.container.entries.TransientMortalCacheValue} that is {@link
 * org.infinispan.container.entries.versioned.Versioned}
 *
 * @author Manik Surtani
 * @since 5.1
 */
public class MetadataTransientMortalCacheValue extends MetadataMortalCacheValue implements MetadataAware {

   long lastUsed;

   public MetadataTransientMortalCacheValue(Object v, Metadata metadata, long created, long lastUsed) {
      super(v, metadata, created);
      this.lastUsed = lastUsed;
   }

   private MetadataTransientMortalCacheValue(CommonData data, Metadata metadata, long created, long lastUsed) {
      super(data, metadata, created);
      this.lastUsed = lastUsed;
   }

   @Override
   public long getMaxIdle() {
      return metadata.maxIdle();
   }

   @Override
   public long getLastUsed() {
      return lastUsed;
   }

   @Override
   public boolean isExpired(long now) {
      return ExpiryHelper.isExpiredTransientMortal(metadata.maxIdle(), lastUsed, metadata.lifespan(), created, now);
   }

   @Override
   public boolean isMaxIdleExpirable() {
      return true;
   }

   @Override
   public long getExpiryTime() {
      long lifespan = metadata.lifespan();
      long lset = lifespan > -1 ? created + lifespan : -1;
      long maxIdle = metadata.maxIdle();
      long muet = maxIdle > -1 ? lastUsed + maxIdle : -1;
      if (lset == -1) {
         return muet;
      }
      if (muet == -1) {
         return lset;
      }
      return min(lset, muet);
   }

   @Override
   protected InternalCacheEntry createEntry(Object key) {
      return new MetadataTransientMortalCacheEntry(key, value, metadata, lastUsed, created);
   }

   @Override
   protected void appendFieldsToString(StringBuilder builder) {
      super.appendFieldsToString(builder);
      builder.append(", lastUsed=").append(lastUsed);
   }

   public static class Externalizer extends AbstractExternalizer<MetadataTransientMortalCacheValue> {
      @Override
      public void writeObject(ObjectOutput output, MetadataTransientMortalCacheValue value) throws IOException {
         writeCommonDataTo(value, output);
         output.writeObject(value.metadata);
         UnsignedNumeric.writeUnsignedLong(output, value.created);
         UnsignedNumeric.writeUnsignedLong(output, value.lastUsed);
      }

      @Override
      public MetadataTransientMortalCacheValue readObject(ObjectInput input)
            throws IOException, ClassNotFoundException {
         CommonData data = readCommonDataFrom(input);
         Metadata metadata = (Metadata) input.readObject();
         long created = UnsignedNumeric.readUnsignedLong(input);
         long lastUsed = UnsignedNumeric.readUnsignedLong(input);
         return new MetadataTransientMortalCacheValue(data, metadata, created, lastUsed);
      }

      @Override
      public Integer getId() {
         return Ids.METADATA_TRANSIENT_MORTAL_VALUE;
      }

      @Override
      public Set<Class<? extends MetadataTransientMortalCacheValue>> getTypeClasses() {
         return Collections.singleton(MetadataTransientMortalCacheValue.class);
      }
   }

}
