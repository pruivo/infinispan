package org.infinispan.xsite.async;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.infinispan.AdvancedCache;
import org.infinispan.metadata.Metadata;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public class AsyncUpdate {

   private final boolean removal;
   private final Object key;
   private final Object value;
   private final Metadata metadata;

   private AsyncUpdate(boolean removal, Object key, Object value, Metadata metadata) {
      this.removal = removal;
      this.key = key;
      this.value = value;
      this.metadata = metadata;
   }

   public static AsyncUpdate update(Object key, Object value, Metadata metadata) {
      return new AsyncUpdate(false, key, value, metadata);
   }

   public static AsyncUpdate removal(Object key) {
      return new AsyncUpdate(true, key, null, null);
   }

   public static void writeTo(ObjectOutput output, AsyncUpdate update) throws IOException {
      output.writeBoolean(update.removal);
      output.writeObject(update.key);
      if (update.removal) {
         return;
      }
      output.writeObject(update.value);
      output.writeObject(update.metadata);
   }

   public static AsyncUpdate readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      if (input.readBoolean()) {
         return removal(input.readObject());
      } else {
         return update(input.readObject(), input.readObject(), (Metadata) input.readObject());
      }
   }

   public void perform(AdvancedCache<Object, Object> cache) {
      if (removal) {
         cache.remove(key);
      } else {
         cache.put(key, value, metadata);
      }
   }

   public Object getKey() {
      return key;
   }
}
