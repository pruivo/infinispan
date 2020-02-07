package org.infinispan.container.versioning.irac;

import java.util.Optional;

import org.infinispan.metadata.impl.IracMetadata;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 10.1
 */
public interface IracVersionGenerator {
   IracMetadata generateNewMetadata(int segment);

   void updateVersion(int segment, IracEntryVersion remoteVersion);

   void storeTombstone(Object key, IracMetadata metadata);

   void storeTombstoneIfAbsent(Object key, IracMetadata metadata);

   Optional<IracMetadata> findTombstone(Object key);

   void removeTombstone(Object key, IracMetadata iracMetadata);
}
