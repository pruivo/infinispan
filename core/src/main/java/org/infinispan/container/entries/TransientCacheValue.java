package org.infinispan.container.entries;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.marshall.core.Ids;

/**
 * A transient cache value, to correspond with {@link org.infinispan.container.entries.TransientCacheEntry}
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class TransientCacheValue extends ImmortalCacheValue {
   protected long maxIdle;
   protected long lastUsed;

   public TransientCacheValue(Object value, long maxIdle, long lastUsed) {
      super(value);
      this.maxIdle = maxIdle;
      this.lastUsed = lastUsed;
   }

   private TransientCacheValue(CommonData data, long maxIdle, long lastUsed) {
      super(data);
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
   public final boolean isExpired(long now) {
      return ExpiryHelper.isExpiredTransient(maxIdle, lastUsed, now);
   }

   @Override
   public boolean canExpire() {
      return true;
   }

   @Override
   public boolean isMaxIdleExpirable() {
      return true;
   }

   @Override
   public long getExpiryTime() {
      return maxIdle > -1 ? lastUsed + maxIdle : -1;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) {
         return true;
      }
      if (!(o instanceof TransientCacheValue)) {
         return false;
      }
      if (!super.equals(o)) {
         return false;
      }

      TransientCacheValue that = (TransientCacheValue) o;

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
   public TransientCacheValue clone() {
      return (TransientCacheValue) super.clone();
   }

   @Override
   protected InternalCacheEntry createEntry(Object key) {
      return new TransientCacheEntry(key, value, maxIdle, lastUsed);
   }

   @Override
   protected void appendFieldsToString(StringBuilder builder) {
      super.appendFieldsToString(builder);
      builder.append(", maxIdle=").append(maxIdle);
      builder.append(", lastUsed=").append(lastUsed);
   }

   public static class Externalizer extends AbstractExternalizer<TransientCacheValue> {
      @Override
      public void writeObject(ObjectOutput output, TransientCacheValue tcv) throws IOException {
         writeCommonDataTo(tcv, output);
         UnsignedNumeric.writeUnsignedLong(output, tcv.lastUsed);
         output.writeLong(tcv.maxIdle); // could be negative so should not use unsigned longs
      }

      @Override
      public TransientCacheValue readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         CommonData data = readCommonDataFrom(input);
         long lastUsed = UnsignedNumeric.readUnsignedLong(input);
         long maxIdle = input.readLong();
         return new TransientCacheValue(data, maxIdle, lastUsed);
      }

      @Override
      public Integer getId() {
         return Ids.TRANSIENT_VALUE;
      }

      @Override
      public Set<Class<? extends TransientCacheValue>> getTypeClasses() {
         return Collections.singleton(TransientCacheValue.class);
      }
   }
}
