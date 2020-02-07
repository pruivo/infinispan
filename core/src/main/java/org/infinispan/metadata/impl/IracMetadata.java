package org.infinispan.metadata.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Objects;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.container.versioning.irac.IracEntryVersion;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.protostream.descriptors.Type;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 10.1
 */
@ProtoTypeId(ProtoStreamTypeIds.IRAC_METADATA)
public class IracMetadata {

   private final String site;
   private final IracEntryVersion version;

   @ProtoFactory
   public IracMetadata(String site, IracEntryVersion version) {
      this.site = Objects.requireNonNull(site);
      this.version = Objects.requireNonNull(version);
   }

   @ProtoField(number = 1, type = Type.STRING, required = true)
   public String getSite() {
      return site;
   }

   @ProtoField(number = 2, required = true)
   public IracEntryVersion getVersion() {
      return version;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) {
         return true;
      }
      if (o == null || getClass() != o.getClass()) {
         return false;
      }

      IracMetadata that = (IracMetadata) o;

      if (!site.equals(that.site)) {
         return false;
      }
      return version.equals(that.version);
   }

   @Override
   public int hashCode() {
      int result = site.hashCode();
      result = 31 * result + version.hashCode();
      return result;
   }

   @Override
   public String toString() {
      return "IracMetadata{" +
             "site='" + site + '\'' +
             ", version=" + version +
             '}';
   }

   public void writeTo(ObjectOutput out) throws IOException {
      out.writeUTF(site);
      out.writeObject(version);
   }

   public static void writeTo(ObjectOutput output, IracMetadata metadata) throws IOException {
      metadata.writeTo(output);
   }

   public static IracMetadata readFrom(ObjectInput in) throws IOException, ClassNotFoundException {
      return new IracMetadata(in.readUTF(), (IracEntryVersion) in.readObject());
   }
}
