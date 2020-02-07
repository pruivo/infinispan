package org.infinispan.container.entries.metadata;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.container.entries.ImmortalCacheValue;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.marshall.core.Ids;
import org.infinispan.metadata.Metadata;

/**
 * A form of {@link org.infinispan.container.entries.ImmortalCacheValue} that is {@link
 * org.infinispan.container.entries.metadata.MetadataAware}
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
public class MetadataImmortalCacheValue extends ImmortalCacheValue implements MetadataAware {

   Metadata metadata;

   public MetadataImmortalCacheValue(Object value, Metadata metadata) {
      super(value);
      this.metadata = metadata;
   }

   private MetadataImmortalCacheValue(CommonData data, Metadata metadata) {
      super(data);
      this.metadata = metadata;
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
   protected InternalCacheEntry createEntry(Object key) {
      return new MetadataImmortalCacheEntry(key, value, metadata);
   }

   @Override
   protected void appendFieldsToString(StringBuilder builder) {
      super.appendFieldsToString(builder);
      builder.append(", metadata=").append(metadata);
   }

   public static class Externalizer extends AbstractExternalizer<MetadataImmortalCacheValue> {
      @Override
      public void writeObject(ObjectOutput output, MetadataImmortalCacheValue icv) throws IOException {
         writeCommonDataTo(icv, output);
         output.writeObject(icv.metadata);
      }

      @Override
      public MetadataImmortalCacheValue readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         CommonData data = readCommonDataFrom(input);
         Metadata metadata = (Metadata) input.readObject();
         return new MetadataImmortalCacheValue(data, metadata);
      }

      @Override
      public Integer getId() {
         return Ids.METADATA_IMMORTAL_VALUE;
      }

      @Override
      public Set<Class<? extends MetadataImmortalCacheValue>> getTypeClasses() {
         return Collections.singleton(MetadataImmortalCacheValue.class);
      }
   }

}
