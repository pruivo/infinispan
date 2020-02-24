package org.infinispan.xsite;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.infinispan.Cache;
import org.infinispan.configuration.ConfigurationManager;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.infinispan.notifications.cachemanagerlistener.annotation.CacheStopped;
import org.infinispan.notifications.cachemanagerlistener.event.CacheStoppedEvent;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @author Mircea Markus
 * @since 5.2
 */
@Listener
@Scope(Scopes.GLOBAL)
public class BackupReceiverRepositoryImpl implements BackupReceiverRepository {

   private static Log log = LogFactory.getLog(BackupReceiverRepositoryImpl.class);

   private final ConcurrentMap<SiteCachePair, BackupReceiver> backupReceivers = new ConcurrentHashMap<>();
   private final ConcurrentMap<String, BackupReceiver> cacheBackupReceivers = new ConcurrentHashMap<>();
   private final Set<String> localCacheName = ConcurrentHashMap.newKeySet();

   @Inject EmbeddedCacheManager cacheManager;
   @Inject CacheManagerNotifier cacheManagerNotifier;
   @Inject ConfigurationManager configurationManager;

   @Start
   public void start() {
      cacheManagerNotifier.addListener(this);
   }

   @Stop
   public void stop() {
      cacheManagerNotifier.removeListener(this);
   }

   @CacheStopped
   public void cacheStopped(CacheStoppedEvent cse) {
      log.debugf("Processing cache stop: %s. Cache name: '%s'", cse, cse.getCacheName());
      for (SiteCachePair scp : backupReceivers.keySet()) {
         log.debugf("Processing entry %s", scp);
         if (scp.localCacheName.equals(cse.getCacheName())) {
            log.debugf("Deregistering backup receiver %s", scp);
            backupReceivers.remove(scp);
         }
      }
      cacheBackupReceivers.remove(cse.getCacheName());
   }

   /**
    * Returns the local cache defined as backup for the provided remote (site, cache) combo, or throws an
    * exception if no such site is defined.
    * <p/>
    * Also starts the cache if not already started; that is because the cache is needed for update after this method
    * is invoked.
    */
   @Override
   public BackupReceiver getBackupReceiver(String remoteSite, String remoteCache) {
      SiteCachePair toLookFor = new SiteCachePair(remoteCache, remoteSite);
      BackupReceiver backupManager = backupReceivers.get(toLookFor);
      if (backupManager != null) return backupManager;

      Set<String> cacheNames = cacheManager.getCacheNames();
      for (String name : cacheNames) {
         Configuration cacheConfiguration = configurationManager.getConfiguration(name, false);
         if (cacheConfiguration != null && isBackupForRemoteCache(remoteSite, remoteCache, cacheConfiguration, name)) {
            checkNotLocalCache(name);
            Cache<Object, Object> cache = cacheManager.getCache(name);
            toLookFor.setLocalCacheName(name);
            BackupReceiver receiver = getBackupReceiver(cache);
            backupReceivers.putIfAbsent(toLookFor, receiver);
            return receiver;
         }
      }
      log.debugf("Did not find any backup explicitly configured backup cache for remote cache/site: %s/%s. Using %s",
                 remoteSite, remoteCache, remoteCache);

      Cache<Object, Object> cache = cacheManager.getCache(remoteCache);
      checkNotLocalCache(cache.getName());
      BackupReceiver receiver = getBackupReceiver(cache);
      backupReceivers.putIfAbsent(toLookFor, receiver);
      toLookFor.setLocalCacheName(cache.getName());
      return receiver;
   }

   @Override
   public BackupReceiver getBackupReceiver(Cache<Object, Object> cache) {
      return cacheBackupReceivers.computeIfAbsent(cache.getName(), (ignored) -> createBackupReceiver(cache));
   }

   private boolean isBackupForRemoteCache(String remoteSite, String remoteCache, Configuration cacheConfiguration, String name) {
      boolean found = cacheConfiguration.sites().backupFor().isBackupFor(remoteSite, remoteCache);
      if (found)
         log.tracef("Found local cache '%s' is backup for cache '%s' from site '%s'", name, remoteCache, remoteSite);
      return found;
   }

   private static class SiteCachePair {
      public final String remoteSite;
      public final String remoteCache;
      public String localCacheName;

      /**
       * Important: do not include the localCacheName field in the equals and hash code comparison. This is mainly used
       * as a key in a map and the localCacheName field might change causing troubles.
       */
      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (!(o instanceof SiteCachePair)) return false;

         SiteCachePair that = (SiteCachePair) o;

         return Objects.equals(remoteCache, that.remoteCache) &&
               Objects.equals(remoteSite, that.remoteSite);
      }

      @Override
      public int hashCode() {
         int result = remoteSite != null ? remoteSite.hashCode() : 0;
         result = 31 * result + (remoteCache != null ? remoteCache.hashCode() : 0);
         return result;
      }

      SiteCachePair(String remoteCache, String remoteSite) {
         this.remoteCache = remoteCache;
         this.remoteSite = remoteSite;
      }

      public void setLocalCacheName(String localCacheName) {
         this.localCacheName = localCacheName;
      }

      @Override
      public String toString() {
         return "SiteCachePair{" +
               "site='" + remoteSite + '\'' +
               ", cache='" + remoteCache + '\'' +
               '}';
      }
   }

   public void replace(String site, String cache, BackupReceiver bcr) {
      backupReceivers.replace(new SiteCachePair(cache, site), bcr);
   }

   public BackupReceiver get(String site, String cache) {
      return backupReceivers.get(new SiteCachePair(site, cache));
   }

   private BackupReceiver createBackupReceiver(Cache<Object,Object> cache) {
      Cache<Object, Object> receiverCache = SecurityActions.getUnwrappedCache(cache);
      if (receiverCache.getCacheConfiguration().clustering().cacheMode().isClustered()) {
         return new ClusteredCacheBackupReceiver(receiverCache);
      }
      localCacheName.add(receiverCache.getName());
      throw log.xsiteInLocalCache();
   }

   private void checkNotLocalCache(String cacheName) {
      if (localCacheName.contains(cacheName)) {
         throw log.xsiteInLocalCache();
      }
   }
}
