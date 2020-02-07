package org.infinispan.remoting.responses;

import static org.infinispan.commons.marshall.MarshallUtil.marshallMap;
import static org.infinispan.commons.marshall.MarshallUtil.unmarshallMap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.marshall.core.Ids;
import org.infinispan.metadata.impl.IracMetadata;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 10.1
 */
public class PrepareResponse extends ValidResponse {

   public static final Externalizer EXTERNALIZER = new Externalizer();

   private EntryVersionsMap newWriteSkewVersions;
   private Map<Integer, IracMetadata> newIracMetadata;

   public static void writeTo(PrepareResponse response, ObjectOutput output) throws IOException {
      marshallMap(response.newWriteSkewVersions, output);
      marshallMap(response.newIracMetadata, DataOutput::writeInt, IracMetadata::writeTo, output);
   }

   public static PrepareResponse readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      PrepareResponse response = new PrepareResponse();
      response.newWriteSkewVersions = unmarshallMap(input, EntryVersionsMap::new);
      response.newIracMetadata = unmarshallMap(input, DataInput::readInt, IracMetadata::readFrom, HashMap::new);
      return response;
   }

   public static PrepareResponse asPrepareResponse(Object rv) {
      assert rv == null || rv instanceof PrepareResponse;
      return rv == null ? new PrepareResponse() : (PrepareResponse) rv;
   }

   @Override
   public boolean isSuccessful() {
      return true;
   }

   @Override
   public boolean isValid() {
      return true;
   }

   @Override
   public Object getResponseValue() {
      return null; //not used!
   }

   @Override
   public String toString() {
      return "PrepareResponse{" +
             "WriteSkewVersions=" + newWriteSkewVersions +
             ", IracMetadataMap=" + newIracMetadata +
             '}';
   }

   public IracMetadata getIracMetadata(int segment) {
      return newIracMetadata != null ? newIracMetadata.get(segment) : null;
   }

   public void setNewIracMetadata(Map<Integer, IracMetadata> map) {
      this.newIracMetadata = map;
   }

   public void merge(PrepareResponse remote) {
      if (remote.newWriteSkewVersions != null) {
         mergeEntryVersions(remote.newWriteSkewVersions);
      }
      if (remote.newIracMetadata != null) {
         if (newIracMetadata == null) {
            newIracMetadata = new HashMap<>(remote.newIracMetadata);
         } else {
            newIracMetadata.putAll(remote.newIracMetadata);
         }
      }
   }

   public EntryVersionsMap mergeEntryVersions(EntryVersionsMap entryVersions) {
      if (newWriteSkewVersions == null) {
         newWriteSkewVersions = new EntryVersionsMap();
      }
      newWriteSkewVersions = newWriteSkewVersions.merge(entryVersions);
      return newWriteSkewVersions;
   }

   private static class Externalizer extends AbstractExternalizer<PrepareResponse> {


      @Override
      public Integer getId() {
         return Ids.PREPARE_RESPONSE;
      }

      @Override
      public Set<Class<? extends PrepareResponse>> getTypeClasses() {
         return Collections.singleton(PrepareResponse.class);
      }

      @Override
      public void writeObject(ObjectOutput output, PrepareResponse object) throws IOException {
         writeTo(object, output);
      }

      @Override
      public PrepareResponse readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return readFrom(input);
      }
   }
}
