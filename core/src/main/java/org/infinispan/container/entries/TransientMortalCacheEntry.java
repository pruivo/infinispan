package org.infinispan.container.entries;

import static java.lang.Math.min;

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
 * A cache entry that is both transient and mortal.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class TransientMortalCacheEntry extends AbstractInternalCacheEntry {

   protected long maxIdle;
   protected long lastUsed;
   protected long lifespan;
   protected long created;

   public TransientMortalCacheEntry(Object key, Object value, long maxIdle, long lifespan, long currentTimeMillis) {
      this(key, value, maxIdle, lifespan, currentTimeMillis, currentTimeMillis);
   }

   public TransientMortalCacheEntry(Object key, Object value, long maxIdle, long lifespan, long lastUsed,
         long created) {
      super(key, value);
      this.maxIdle = maxIdle;
      this.lifespan = lifespan;
      this.created = created;
      this.lastUsed = lastUsed;
   }

   private TransientMortalCacheEntry(CommonData data, long maxIdle, long lifespan, long lastUsed, long created) {
      super(data);
      this.maxIdle = maxIdle;
      this.lifespan = lifespan;
      this.created = created;
      this.lastUsed = lastUsed;
   }

   @Override
   public long getLifespan() {
      return lifespan;
   }

   public void setLifespan(long lifespan) {
      this.lifespan = lifespan;
   }

   @Override
   public final boolean canExpire() {
      return true;
   }

   @Override
   public boolean canExpireMaxIdle() {
      return true;
   }

   @Override
   public long getCreated() {
      return created;
   }

   @Override
   public boolean isExpired(long now) {
      return ExpiryHelper.isExpiredTransientMortal(maxIdle, lastUsed, lifespan, created, now);
   }

   @Override
   public final long getExpiryTime() {
      long lset = lifespan > -1 ? created + lifespan : -1;
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
   public long getLastUsed() {
      return lastUsed;
   }

   @Override
   public final void touch(long currentTimeMillis) {
      this.lastUsed = currentTimeMillis;
   }

   @Override
   public void reincarnate(long now) {
      this.created = now;
   }

   @Override
   public long getMaxIdle() {
      return maxIdle;
   }

   public void setMaxIdle(long maxIdle) {
      this.maxIdle = maxIdle;
   }

   @Override
   public Metadata getMetadata() {
      return new EmbeddedMetadata.Builder()
            .lifespan(lifespan)
            .maxIdle(maxIdle).build();
   }

   @Override
   public void setMetadata(Metadata metadata) {
      throw new IllegalStateException(
            "Metadata cannot be set on mortal entries. They need to be recreated via the entry factory.");
   }

   @Override
   public TransientMortalCacheEntry clone() {
      return (TransientMortalCacheEntry) super.clone();
   }

   @Override
   protected InternalCacheValue createCacheValue() {
      return new TransientMortalCacheValue(value, created, lifespan, maxIdle, lastUsed);
   }

   @Override
   protected void appendFieldsToString(StringBuilder builder) {
      super.appendFieldsToString(builder);
      builder.append(", created=").append(created);
      builder.append(", lifespan=").append(lifespan);
      builder.append(", lastUsed=").append(lastUsed);
      builder.append(", maxIdle=").append(maxIdle);
   }

   public static class Externalizer extends AbstractExternalizer<TransientMortalCacheEntry> {
      @Override
      public void writeObject(ObjectOutput output, TransientMortalCacheEntry entry) throws IOException {
         writeCommonDataTo(entry, output);
         UnsignedNumeric.writeUnsignedLong(output, entry.created);
         output.writeLong(entry.lifespan); // could be negative so should not use unsigned longs
         UnsignedNumeric.writeUnsignedLong(output, entry.lastUsed);
         output.writeLong(entry.maxIdle); // could be negative so should not use unsigned longs
      }

      @Override
      public TransientMortalCacheEntry readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         CommonData data = readCommonDataFrom(input);
         long created = UnsignedNumeric.readUnsignedLong(input);
         long lifespan = input.readLong();
         long lastUsed = UnsignedNumeric.readUnsignedLong(input);
         long maxIdle = input.readLong();
         return new TransientMortalCacheEntry(data, maxIdle, lifespan, lastUsed, created);
      }

      @Override
      public Integer getId() {
         return Ids.TRANSIENT_MORTAL_ENTRY;
      }

      @Override
      public Set<Class<? extends TransientMortalCacheEntry>> getTypeClasses() {
         return Collections.singleton(TransientMortalCacheEntry.class);
      }
   }
}
