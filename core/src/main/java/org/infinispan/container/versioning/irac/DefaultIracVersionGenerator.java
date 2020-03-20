package org.infinispan.container.versioning.irac;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.notifications.cachelistener.annotation.TopologyChanged;
import org.infinispan.notifications.cachelistener.event.TopologyChangedEvent;
import org.infinispan.remoting.transport.Transport;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 10.1
 */
@Listener
@Scope(Scopes.NAMED_CACHE)
public class DefaultIracVersionGenerator implements IracVersionGenerator {

   private final Map<Integer, Map<String, TopologyIracVersion>> segmentVersion;
   private final Map<Object, IracMetadata> tombstone;
   @Inject
   CacheNotifier<?, ?> cacheNotifier;
   @Inject
   Transport transport;
   private String localSite;
   private volatile int topologyId;

   public DefaultIracVersionGenerator() {
      this.segmentVersion = new ConcurrentHashMap<>();
      this.tombstone = new ConcurrentHashMap<>();
   }

   @Start
   @Override
   public void start() {
      transport.checkCrossSiteAvailable();
      localSite = transport.localSiteName();
      cacheNotifier.removeListener(this);
      cacheNotifier.addListener(this);
   }

   @Stop
   @Override
   public void stop() {
      cacheNotifier.removeListener(this);
   }

   @Override
   public IracMetadata generateNewMetadata(int segment) {
      Map<String, TopologyIracVersion> v = segmentVersion.compute(segment, this::generateNewVectorFunction);
      return new IracMetadata(localSite, new IracEntryVersion(v));
   }

   @Override
   public void updateVersion(int segment, IracEntryVersion remoteVersion) {
      segmentVersion.merge(segment, remoteVersion.toMap(), this::mergeVectorsFunction);
   }

   @TopologyChanged
   public void onTopologyChange(TopologyChangedEvent<?, ?> tce) {
      topologyId = tce.getNewTopologyId();
   }

   @Override
   public void storeTombstone(Object key, IracMetadata metadata) {
      tombstone.put(key, metadata);
   }

   @Override
   public void storeTombstoneIfAbsent(Object key, IracMetadata metadata) {
      if (metadata == null) {
         return;
      }
      tombstone.putIfAbsent(key, metadata);
   }

   @Override
   public Optional<IracMetadata> findTombstone(Object key) {
      return Optional.ofNullable(tombstone.get(key));
   }

   @Override
   public void removeTombstone(Object key, IracMetadata iracMetadata) {
      if (iracMetadata == null) {
         return;
      }
      tombstone.remove(key, iracMetadata);
   }

   private Map<String, TopologyIracVersion> generateNewVectorFunction(Integer s,
         Map<String, TopologyIracVersion> versions) {
      if (versions == null) {
         return Collections.singletonMap(localSite, TopologyIracVersion.newVersion(topologyId));
      } else {
         Map<String, TopologyIracVersion> copy = new HashMap<>(versions);
         copy.compute(localSite, this::incrementVersionFunction);
         return Collections.unmodifiableMap(copy);
      }
   }

   private TopologyIracVersion incrementVersionFunction(String site, TopologyIracVersion version) {
      return version == null ? TopologyIracVersion.newVersion(topologyId) : version.increment(topologyId);
   }

   private Map<String, TopologyIracVersion> mergeVectorsFunction(Map<String, TopologyIracVersion> v1,
         Map<String, TopologyIracVersion> v2) {
      if (v1 == null) {
         return v2;
      } else {
         Map<String, TopologyIracVersion> copy = new HashMap<>(v1);
         for (Map.Entry<String, TopologyIracVersion> entry : v2.entrySet()) {
            copy.merge(entry.getKey(), entry.getValue(), TopologyIracVersion::max);
         }
         return Collections.unmodifiableMap(copy);
      }
   }
}
