package org.infinispan.xsite.irac;

import java.util.Collection;
import java.util.Optional;
import java.util.stream.Stream;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.util.IntSet;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.CacheTopology;
import org.infinispan.transaction.xa.GlobalTransaction;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 10.1
 */
@Scope(Scopes.NAMED_CACHE)
public class ControlledIracManager implements IracManager {

   protected final IracManager actual;

   public ControlledIracManager(IracManager actual) {
      this.actual = actual;
   }

   @Stop
   public void stop() {
      asDefaultIracManager().ifPresent(DefaultIracManager::stop);
   }

   @Override
   public void trackUpdatedKey(Object key, Object lockOwner) {
      actual.trackUpdatedKey(key, lockOwner);
   }

   @Override
   public <K> void trackUpdatedKeys(Collection<K> keys, Object lockOwner) {
      actual.trackUpdatedKeys(keys, lockOwner);
   }

   @Override
   public void trackKeysFromTransaction(Stream<WriteCommand> modifications, GlobalTransaction lockOwner) {
      actual.trackKeysFromTransaction(modifications, lockOwner);
   }

   @Override
   public void trackClear() {
      actual.trackClear();
   }

   @Override
   public void cleanupKey(Object key, Object lockOwner) {
      actual.cleanupKey(key, lockOwner);
   }

   @Override
   public String getLocalSiteName() {
      return actual.getLocalSiteName();
   }

   @Override
   public void onTopologyUpdate(CacheTopology oldCacheTopology, CacheTopology newCacheTopology) {
      actual.onTopologyUpdate(oldCacheTopology, newCacheTopology);
   }

   @Override
   public void requestState(Address origin, IntSet segments) {
      actual.requestState(origin, segments);
   }

   @Override
   public void receiveState(Object key, Object lockOwner, IracMetadata tombstone) {
      actual.receiveState(key, lockOwner, tombstone);
   }

   protected Optional<DefaultIracManager> asDefaultIracManager() {
      return actual instanceof DefaultIracManager ? Optional.of((DefaultIracManager) actual) : Optional.empty();
   }
}
