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

/**
 * A transient, mortal cache value to correspond with {@link org.infinispan.container.entries.TransientMortalCacheEntry}
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class TransientMortalCacheValue extends MortalCacheValue {
   protected long maxIdle;
   protected long lastUsed;

   public TransientMortalCacheValue(Object value, long created, long lifespan, long maxIdle, long lastUsed) {
      this(value, created, lifespan, maxIdle);
      this.lastUsed = lastUsed;
   }

   public TransientMortalCacheValue(Object value, long created, long lifespan, long maxIdle) {
      super(value, created, lifespan);
      this.maxIdle = maxIdle;
   }

   private TransientMortalCacheValue(CommonData data, long created, long lifespan, long maxIdle, long lastUsed) {
      super(data, created, lifespan);
      this.maxIdle = maxIdle;
      this.lastUsed = lastUsed;
   }

   @Override
   public long getMaxIdle() {
      return maxIdle;
   }

   public void setMaxIdle(long maxIdle) {
      this.maxIdle = maxIdle;
   }

   @Override
   public long getLastUsed() {
      return lastUsed;
   }

   public void setLastUsed(long lastUsed) {
      this.lastUsed = lastUsed;
   }

   @Override
   public boolean isExpired(long now) {
      return ExpiryHelper.isExpiredTransientMortal(maxIdle, lastUsed, lifespan, created, now);
   }

   @Override
   public boolean isMaxIdleExpirable() {
      return true;
   }

   @Override
   public long getExpiryTime() {
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
   public boolean equals(Object o) {
      if (this == o) {
         return true;
      }
      if (!(o instanceof TransientMortalCacheValue)) {
         return false;
      }
      if (!super.equals(o)) {
         return false;
      }

      TransientMortalCacheValue that = (TransientMortalCacheValue) o;

      return lastUsed == that.lastUsed && maxIdle == that.maxIdle;
   }

   @Override
   public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (int) (maxIdle ^ (maxIdle >>> 32));
      result = 31 * result + (int) (lastUsed ^ (lastUsed >>> 32));
      return result;
   }

   @Override
   public TransientMortalCacheValue clone() {
      return (TransientMortalCacheValue) super.clone();
   }

   @Override
   protected InternalCacheEntry createEntry(Object key) {
      return new TransientMortalCacheEntry(key, value, maxIdle, lifespan, lastUsed, created);
   }

   @Override
   protected void appendFieldsToString(StringBuilder builder) {
      super.appendFieldsToString(builder);
      builder.append(", maxIdle=").append(maxIdle);
      builder.append(", lastUsed=").append(lastUsed);
   }

   public static class Externalizer extends AbstractExternalizer<TransientMortalCacheValue> {
      @Override
      public void writeObject(ObjectOutput output, TransientMortalCacheValue value) throws IOException {
         writeCommonDataTo(value, output);
         UnsignedNumeric.writeUnsignedLong(output, value.created);
         output.writeLong(value.lifespan); // could be negative so should not use unsigned longs
         UnsignedNumeric.writeUnsignedLong(output, value.lastUsed);
         output.writeLong(value.maxIdle); // could be negative so should not use unsigned longs
      }

      @Override
      public TransientMortalCacheValue readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         CommonData data = readCommonDataFrom(input);
         long created = UnsignedNumeric.readUnsignedLong(input);
         long lifespan = input.readLong();
         long lastUsed = UnsignedNumeric.readUnsignedLong(input);
         long maxIdle = input.readLong();
         return new TransientMortalCacheValue(data, created, lifespan, maxIdle, lastUsed);
      }

      @Override
      public Integer getId() {
         return Ids.TRANSIENT_MORTAL_VALUE;
      }

      @Override
      public Set<Class<? extends TransientMortalCacheValue>> getTypeClasses() {
         return Collections.singleton(TransientMortalCacheValue.class);
      }
   }
}
