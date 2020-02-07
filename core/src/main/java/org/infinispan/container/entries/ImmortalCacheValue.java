package org.infinispan.container.entries;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.functional.impl.MetaParamsInternalMetadata;
import org.infinispan.marshall.core.Ids;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;

/**
 * An immortal cache value, to correspond with {@link org.infinispan.container.entries.ImmortalCacheEntry}
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class ImmortalCacheValue implements InternalCacheValue, Cloneable {

   public Object value;
   private MetaParamsInternalMetadata internalMetadata;

   public ImmortalCacheValue(Object value) {
      this.value = value;
   }

   protected ImmortalCacheValue(CommonData data) {
      this.value = data.value;
      this.internalMetadata = data.internalMetadata;
   }

   protected static void writeCommonDataTo(ImmortalCacheValue icv, ObjectOutput output) throws IOException {
      output.writeObject(icv.value);
      output.writeObject(icv.internalMetadata);
   }

   protected static CommonData readCommonDataFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      Object v = input.readObject();
      MetaParamsInternalMetadata internalMetadata = (MetaParamsInternalMetadata) input.readObject();
      return new CommonData(v, internalMetadata);
   }

   @Override
   public final InternalCacheEntry toInternalCacheEntry(Object key) {
      InternalCacheEntry cacheEntry = createEntry(key);
      cacheEntry.setInternalMetadata(getInternalMetadata());
      return cacheEntry;
   }

   public final Object setValue(Object value) {
      Object old = this.value;
      this.value = value;
      return old;
   }

   @Override
   public Object getValue() {
      return value;
   }

   @Override
   public boolean isExpired(long now) {
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
   public long getExpiryTime() {
      return -1;
   }

   @Override
   public Metadata getMetadata() {
      return new EmbeddedMetadata.Builder().lifespan(getLifespan()).maxIdle(getMaxIdle()).build();
   }

   @Override
   public MetaParamsInternalMetadata getInternalMetadata() {
      return internalMetadata;
   }

   @Override
   public void setInternalMetadata(MetaParamsInternalMetadata internalMetadata) {
      this.internalMetadata = internalMetadata;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof ImmortalCacheValue)) return false;

      ImmortalCacheValue that = (ImmortalCacheValue) o;

      return Objects.equals(value, that.value) &&
             Objects.equals(internalMetadata, that.internalMetadata);
   }

   @Override
   public int hashCode() {
      int result = Objects.hashCode(value);
      result = 31 * result + Objects.hashCode(internalMetadata);
      return result;
   }

   @Override
   public final String toString() {
      StringBuilder builder = new StringBuilder(getClass().getSimpleName());
      builder.append('{');
      appendFieldsToString(builder);
      return builder.append('}').toString();
   }

   @Override
   public ImmortalCacheValue clone() {
      try {
         return (ImmortalCacheValue) super.clone();
      } catch (CloneNotSupportedException e) {
         throw new RuntimeException("Should never happen", e);
      }
   }

   protected InternalCacheEntry createEntry(Object key) {
      return new ImmortalCacheEntry(key, value);
   }

   protected void appendFieldsToString(StringBuilder builder) {
      builder.append("value=").append(Util.toStr(value));
      builder.append(", internalMetadata=").append(internalMetadata);
   }

   public static class Externalizer extends AbstractExternalizer<ImmortalCacheValue> {
      @Override
      public void writeObject(ObjectOutput output, ImmortalCacheValue icv) throws IOException {
         writeCommonDataTo(icv, output);
      }

      @Override
      public ImmortalCacheValue readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new ImmortalCacheValue(readCommonDataFrom(input));
      }

      @Override
      public Integer getId() {
         return Ids.IMMORTAL_VALUE;
      }

      @Override
      public Set<Class<? extends ImmortalCacheValue>> getTypeClasses() {
         return Collections.singleton(ImmortalCacheValue.class);
      }
   }

   protected static class CommonData {
      private final Object value;
      private final MetaParamsInternalMetadata internalMetadata;

      public CommonData(Object value, MetaParamsInternalMetadata internalMetadata) {
         this.value = value;
         this.internalMetadata = internalMetadata;
      }
   }
}
