package org.infinispan.container.entries.metadata;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.marshall.core.Ids;
import org.infinispan.metadata.Metadata;

/**
 * A form of {@link org.infinispan.container.entries.ImmortalCacheEntry} that is {@link
 * org.infinispan.container.entries.metadata.MetadataAware}
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
public class MetadataImmortalCacheEntry extends ImmortalCacheEntry implements MetadataAware {

   protected Metadata metadata;

   public MetadataImmortalCacheEntry(Object key, Object value, Metadata metadata) {
      super(key, value);
      this.metadata = metadata;
   }

   private MetadataImmortalCacheEntry(CommonData data, Metadata metadata) {
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
   protected InternalCacheValue createCacheValue() {
      return new MetadataImmortalCacheValue(value, metadata);
   }

   @Override
   protected void appendFieldsToString(StringBuilder builder) {
      super.appendFieldsToString(builder);
      builder.append(", metadata=").append(metadata);
   }

   public static class Externalizer extends AbstractExternalizer<MetadataImmortalCacheEntry> {
      @Override
      public void writeObject(ObjectOutput output, MetadataImmortalCacheEntry ice) throws IOException {
         writeCommonDataTo(ice, output);
         output.writeObject(ice.metadata);
      }

      @Override
      public MetadataImmortalCacheEntry readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         CommonData data = readCommonDataFrom(input);
         Metadata metadata = (Metadata) input.readObject();
         return new MetadataImmortalCacheEntry(data, metadata);
      }

      @Override
      public Integer getId() {
         return Ids.METADATA_IMMORTAL_ENTRY;
      }

      @Override
      public Set<Class<? extends MetadataImmortalCacheEntry>> getTypeClasses() {
         return Collections.singleton(MetadataImmortalCacheEntry.class);
      }
   }
}
