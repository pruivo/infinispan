package org.infinispan.container.entries;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.marshall.core.Ids;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;

/**
 * A cache entry that is mortal.  I.e., has a lifespan.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class MortalCacheEntry extends AbstractInternalCacheEntry {

   protected long lifespan;
   protected long created;

   public MortalCacheEntry(Object key, Object value, long lifespan, long created) {
      super(key, value);
      this.lifespan = lifespan;
      this.created = created;
   }

   private MortalCacheEntry(CommonData data, long lifespan, long created) {
      super(data);
      this.lifespan = lifespan;
      this.created = created;
   }

   @Override
   public final boolean isExpired(long now) {
      return ExpiryHelper.isExpiredMortal(lifespan, created, now);
   }

   @Override
   public final boolean canExpire() {
      return true;
   }

   @Override
   public final long getCreated() {
      return created;
   }

   @Override
   public final long getLastUsed() {
      return -1;
   }

   @Override
   public final long getLifespan() {
      return lifespan;
   }

   public void setLifespan(long lifespan) {
      this.lifespan = lifespan;
   }

   @Override
   public final long getMaxIdle() {
      return -1;
   }

   @Override
   public final long getExpiryTime() {
      return lifespan > -1 ? created + lifespan : -1;
   }

   @Override
   public final void touch(long currentTimeMillis) {
      // no-op
   }

   @Override
   public void reincarnate(long now) {
      this.created = now;
   }

   @Override
   public Metadata getMetadata() {
      return new EmbeddedMetadata.Builder().lifespan(lifespan).build();
   }

   @Override
   public void setMetadata(Metadata metadata) {
      throw new IllegalStateException(
            "Metadata cannot be set on mortal entries. They need to be recreated via the entry factory.");
   }

   @Override
   public MortalCacheEntry clone() {
      return (MortalCacheEntry) super.clone();
   }

   @Override
   protected InternalCacheValue createCacheValue() {
      return new MortalCacheValue(value, created, lifespan);
   }

   @Override
   protected void appendFieldsToString(StringBuilder builder) {
      super.appendFieldsToString(builder);
      builder.append(", created=").append(created);
      builder.append(", lifespan=").append(lifespan);
   }

   public static class Externalizer extends AbstractExternalizer<MortalCacheEntry> {
      @Override
      public void writeObject(ObjectOutput output, MortalCacheEntry mce) throws IOException {
         writeCommonDataTo(mce, output);
         UnsignedNumeric.writeUnsignedLong(output, mce.created);
         output.writeLong(mce.lifespan); // could be negative so should not use unsigned longs
      }

      @Override
      public MortalCacheEntry readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         CommonData data = readCommonDataFrom(input);
         long created = UnsignedNumeric.readUnsignedLong(input);
         long lifespan = input.readLong();
         return new MortalCacheEntry(data, lifespan, created);
      }

      @Override
      public Integer getId() {
         return Ids.MORTAL_ENTRY;
      }

      @Override
      public Set<Class<? extends MortalCacheEntry>> getTypeClasses() {
         return Collections.singleton(MortalCacheEntry.class);
      }
   }
}
