package org.infinispan.xsite.irac;

import java.util.Optional;

import org.infinispan.container.versioning.irac.IracEntryVersion;
import org.infinispan.container.versioning.irac.IracVersionGenerator;
import org.infinispan.metadata.impl.IracMetadata;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 10.1
 */
public class ControlledIracVersionGenerator implements IracVersionGenerator {

   private final IracVersionGenerator actual;


   public ControlledIracVersionGenerator(IracVersionGenerator actual) {
      this.actual = actual;
   }

   @Override
   public IracMetadata generateNewMetadata(int segment) {
      return actual.generateNewMetadata(segment);
   }

   @Override
   public void updateVersion(int segment, IracEntryVersion remoteVersion) {
      actual.updateVersion(segment, remoteVersion);
   }

   @Override
   public void storeTombstone(Object key, IracMetadata metadata) {
      actual.storeTombstone(key, metadata);
   }

   @Override
   public void storeTombstoneIfAbsent(Object key, IracMetadata metadata) {
      actual.storeTombstoneIfAbsent(key, metadata);
   }

   @Override
   public Optional<IracMetadata> findTombstone(Object key) {
      return actual.findTombstone(key);
   }

   @Override
   public void removeTombstone(Object key, IracMetadata iracMetadata) {
      actual.removeTombstone(key, iracMetadata);
   }

   @Override
   public void start() {
      actual.start();
   }

   @Override
   public void stop() {
      actual.stop();
   }
}
