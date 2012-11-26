package org.infinispan.container.entries;

import org.infinispan.container.DataContainer;
import org.infinispan.container.versioning.EntryVersion;

/**
 * @author Pedro Ruivo
 * @author Sebastiano Peluso
 * @since 5.2
 */
public class SerializableEntry extends RepeatableReadEntry {

   public SerializableEntry(Object key, Object value, long lifespan, EntryVersion version) {
      super(key, value, version, lifespan);
   }

   @Override
   public void performLocalWriteSkewCheck(DataContainer container, boolean alreadyCopied) {
      //no-op
   }
}
