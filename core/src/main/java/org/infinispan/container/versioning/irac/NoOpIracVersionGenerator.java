package org.infinispan.container.versioning.irac;

import java.util.Optional;

import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.metadata.impl.IracMetadata;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 11.1
 */
@Scope(Scopes.NAMED_CACHE)
public class NoOpIracVersionGenerator implements IracVersionGenerator {

   private static final NoOpIracVersionGenerator INSTANCE = new NoOpIracVersionGenerator();

   private NoOpIracVersionGenerator() {
   }

   public static NoOpIracVersionGenerator getInstance() {
      return INSTANCE;
   }

   @Override
   public IracMetadata generateNewMetadata(int segment) {
      throw new IllegalStateException(); //if we don't have IRAC enabled, this shouldn't be invoked.
   }

   @Override
   public void updateVersion(int segment, IracEntryVersion remoteVersion) {
      //no-op
   }

   @Override
   public void storeTombstone(Object key, IracMetadata metadata) {
      //no-op
   }

   @Override
   public void storeTombstoneIfAbsent(Object key, IracMetadata metadata) {
      //no-op
   }

   @Override
   public Optional<IracMetadata> findTombstone(Object key) {
      return Optional.empty(); //no-op
   }

   @Override
   public void removeTombstone(Object key, IracMetadata iracMetadata) {
      //no-op
   }

   @Override
   public void start() {
      //no-op
   }

   @Override
   public void stop() {
      //no-op
   }
}
