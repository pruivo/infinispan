package org.infinispan.container.entries;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;
import java.util.Objects;

import org.infinispan.commons.util.Util;
import org.infinispan.container.DataContainer;
import org.infinispan.functional.impl.MetaParamsInternalMetadata;
import org.infinispan.metadata.Metadata;

/**
 * An abstract internal cache entry that is typically stored in the data container
 *
 * @author Manik Surtani
 * @since 4.0
 */
public abstract class AbstractInternalCacheEntry implements InternalCacheEntry {

   protected Object key;
   protected Object value;
   protected MetaParamsInternalMetadata internalMetadata;

   protected AbstractInternalCacheEntry(Object key, Object value) {
      this.key = key;
      this.value = value;
   }

   protected AbstractInternalCacheEntry(CommonData data) {
      this.key = data.key;
      this.value = data.value;
      this.internalMetadata = data.internalMetadata;
   }

   @Override
   public final void commit(DataContainer container) {
      // no-op
   }

   @Override
   public void setChanged(boolean changed) {
      // no-op
   }

   @Override
   public final void setCreated(boolean created) {
      // no-op
   }

   @Override
   public final void setRemoved(boolean removed) {
      // no-op
   }

   @Override
   public final void setEvicted(boolean evicted) {
      // no-op
   }

   @Override
   public void setSkipLookup(boolean skipLookup) {
      //no-op
   }

   @Override
   public final boolean isNull() {
      return false;
   }

   @Override
   public final boolean isChanged() {
      return false;
   }

   @Override
   public final boolean isCreated() {
      return false;
   }

   @Override
   public final boolean isRemoved() {
      return false;
   }

   @Override
   public final boolean isEvicted() {
      return true;
   }

   @Override
   public boolean skipLookup() {
      return true;
   }

   @Override
   public Metadata getMetadata() {
      return null;
   }

   @Override
   public void setMetadata(Metadata metadata) {
      // no-op
   }

   @Override
   public final Object getKey() {
      return key;
   }

   @Override
   public final Object getValue() {
      return value;
   }

   @Override
   public final Object setValue(Object value) {
     return this.value = value;
   }

   @Override
   public boolean isL1Entry() {
      return false;
   }

   @Override
   public MetaParamsInternalMetadata getInternalMetadata() {
      return internalMetadata;
   }

   @Override
   public void setInternalMetadata(MetaParamsInternalMetadata metadata) {
      this.internalMetadata = metadata;
   }

   @Override
   public final InternalCacheValue toInternalCacheValue() {
      InternalCacheValue cv = createCacheValue();
      cv.setInternalMetadata(internalMetadata);
      return cv;
   }

   @Override
   public final String toString() {
      StringBuilder sb = new StringBuilder(getClass().getSimpleName());
      sb.append('{');
      appendFieldsToString(sb);
      return sb.append('}').toString();
   }

   @Override
   public AbstractInternalCacheEntry clone() {
      try {
         return (AbstractInternalCacheEntry) super.clone();
      } catch (CloneNotSupportedException e) {
         throw new RuntimeException("Should never happen!", e);
      }
   }

   @Override
   public final boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || !(o instanceof Map.Entry)) return false;

      Map.Entry that = (Map.Entry) o;

      return Objects.equals(getKey(), that.getKey()) && Objects.equals(getValue(), that.getValue());
   }

   @Override
   public final int hashCode() {
      return  31 * Objects.hashCode(getKey()) + Objects.hashCode(getValue());
   }

   protected abstract InternalCacheValue createCacheValue();

   protected void appendFieldsToString(StringBuilder builder) {
      builder.append("key=").append(Util.toStr(key));
      builder.append(", value=").append(Util.toStr(value));
      builder.append(", internalMetadata=").append(internalMetadata);
   }

   protected static void writeCommonDataTo(AbstractInternalCacheEntry cacheEntry, ObjectOutput output)
         throws IOException {
      output.writeObject(cacheEntry.key);
      output.writeObject(cacheEntry.value);
      output.writeObject(cacheEntry.internalMetadata);
   }

   protected static CommonData readCommonDataFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      Object key = input.readObject();
      Object value = input.readObject();
      MetaParamsInternalMetadata internalMetadata = (MetaParamsInternalMetadata) input.readObject();
      return new CommonData(key, value, internalMetadata);
   }

   protected static class CommonData {
      protected final Object key;
      protected final Object value;
      protected final MetaParamsInternalMetadata internalMetadata;

      public CommonData(Object key, Object value,
            MetaParamsInternalMetadata internalMetadata) {
         this.key = key;
         this.value = value;
         this.internalMetadata = internalMetadata;
      }
   }
}
