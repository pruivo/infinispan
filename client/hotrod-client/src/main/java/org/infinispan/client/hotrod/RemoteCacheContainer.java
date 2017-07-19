package org.infinispan.client.hotrod;

import javax.transaction.TransactionManager;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.TransactionMode;
import org.infinispan.commons.api.BasicCacheContainer;
import org.infinispan.commons.marshall.Marshaller;

public interface RemoteCacheContainer extends BasicCacheContainer {

   /**
    * Retrieves the configuration currently in use. The configuration object
    * is immutable. If you wish to change configuration, you should use the
    * following pattern:
    *
    * <pre><code>
    * ConfigurationBuilder builder = new ConfigurationBuilder();
    * builder.read(remoteCacheManager.getConfiguration());
    * // modify builder
    * remoteCacheManager.stop();
    * remoteCacheManager = new RemoteCacheManager(builder.build());
    * </code></pre>
    *
    * @return The configuration of this RemoteCacheManager
    */
   Configuration getConfiguration();

   <K, V> RemoteCache<K, V> getCache(String cacheName, boolean forceReturnValue);

   <K, V> RemoteCache<K, V> getCache(boolean forceReturnValue);

   default <K, V> RemoteCache<K, V> getTransactionalCache(String cacheName) {
      return getTransactionalCache(cacheName, null, null);
   }

   default <K, V> RemoteCache<K, V> getTransactionalCache(String cacheName, boolean forceReturnValue) {
      return getTransactionalCache(cacheName, forceReturnValue, null, null);
   }

   <K, V> RemoteCache<K, V> getTransactionalCache(String cacheName, TransactionManager transactionManager,
         TransactionMode transactionMode);

   <K, V> RemoteCache<K, V> getTransactionalCache(String cacheName, boolean forceReturnValue,
         TransactionManager transactionManager, TransactionMode transactionMode);

   boolean isStarted();

   /**
    * Switch remote cache manager to a different cluster, previously
    * declared via configuration. If the switch was completed successfully,
    * this method returns {@code true}, otherwise it returns {@code false}.
    *
    * @param clusterName name of the cluster to which to switch to
    * @return {@code true} if the cluster was switched, {@code false} otherwise
    */
   boolean switchToCluster(String clusterName);

   /**
    * Switch remote cache manager to a the default cluster, previously
    * declared via configuration. If the switch was completed successfully,
    * this method returns {@code true}, otherwise it returns {@code false}.
    *
    * @return {@code true} if the cluster was switched, {@code false} otherwise
    */
   boolean switchToDefaultCluster();

   Marshaller getMarshaller();
}
