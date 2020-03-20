package org.infinispan.xsite.irac;

import java.util.Collection;
import java.util.stream.Stream;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.util.IntSet;
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
 * @since 8.0
 */
@Scope(Scopes.NAMED_CACHE)
public class NoOpIracManager implements IracManager {

   private static final NoOpIracManager INSTANCE = new NoOpIracManager();

   private NoOpIracManager() {
   }

   public static NoOpIracManager getInstance() {
      return INSTANCE;
   }

   @Override
   public void trackUpdatedKey(Object key, Object lockOwner) {
      //no-op
   }

   @Override
   public <K> void trackUpdatedKeys(Collection<K> keys, Object lockOwner) {
      //no-op
   }

   @Override
   public void trackKeysFromTransaction(Stream<WriteCommand> modifications, GlobalTransaction lockOwner) {
      //no-op
   }

   @Override
   public void trackClear() {
      //no-op
   }

   @Override
   public void cleanupKey(Object key, Object lockOwner, IracMetadata tombstone) {
      //no-op
   }

   @Override
   public void onTopologyUpdate(CacheTopology oldCacheTopology, CacheTopology newCacheTopology) {
      //no-op
   }

   @Override
   public String getLocalSiteName() {
      return null;
   }

   @Override
   public void requestState(Address origin, IntSet segments) {
      //no-op
   }

   @Override
   public void receiveState(Object key, Object lockOwner, IracMetadata tombstone) {
      //no-op
   }
}
