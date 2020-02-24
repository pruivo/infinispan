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
public interface IracManager {

   void trackUpdatedKey(Object key, Object lockOwner);

   <K> void  trackUpdatedKeys(Collection<K> keys, Object lockOwner);

   void trackKeysFromTransaction(Stream<WriteCommand> modifications, GlobalTransaction lockOwner);

   void trackClear();

   void cleanupKey(Object key, Object lockOwner);

   void onTopologyUpdate(CacheTopology oldCacheTopology, CacheTopology newCacheTopology);

   String getLocalSiteName();

   void requestState(Address origin, IntSet segments);

   void receiveState(Object key, Object lockOwner, IracMetadata tombstone);

}
