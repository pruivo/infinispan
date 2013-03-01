package org.infinispan.dataplacement;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.dataplacement.hm.HashMapObjectLookupFactory;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.interceptors.TxInterceptor;
import org.infinispan.stats.topK.StreamLibContainer;
import org.infinispan.stats.topK.StreamLibInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Simple movement test
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", description = "something fancy", testName = "dataplacement.DataPlacementTest")
public class DataPlacementTest extends MultipleCacheManagersTest {

   private static final int NUMBER_OF_KEYS = 1000;
   private static final int NUMBER_OF_ITERATIONS = 100;

   public void testKeySwap() throws Exception {
      //init 
      for (int i = 0; i < NUMBER_OF_KEYS; ++i) {
         cache(0).put(getKey(i), i % 2); //dummy value
      }

      //reset top key in both caches
      resetTopKey();

      //cache 0 will read the even keys and the cache 1 the odd keys      
      for (int key = 0; key < NUMBER_OF_KEYS; ++key) {
         for (int iteration = 0; iteration < NUMBER_OF_ITERATIONS; ++iteration) {
            cache(key % 2).get(getKey(key));
         }
      }

      //trigger data placement: we are using the hash map to avoid errors in location
      triggerDataPlacement();

      //TODO how to wait for the data placement to finish?
      TestingUtil.sleepThread(60000);

      //check keys position (data container, consistent hash)
      for (int key = 0; key < NUMBER_OF_KEYS; ++key) {
         assertKeyLocation(key % 2, getKey(key));
      }
   }

   private String getKey(int i) {
      return "KEY_" + i;
   }

   private void resetTopKey(Cache... caches) {
      for (Cache cache : caches) {
         StreamLibContainer.getOrCreateStreamLibContainer(cache).resetAll();
      }
   }

   private void resetTopKey() {
      for (Cache cache : caches()) {
         StreamLibContainer.getOrCreateStreamLibContainer(cache).resetAll();
      }
   }

   private void triggerDataPlacement() throws Exception {
      TestingUtil.extractComponent(cache(0), DataPlacementManager.class).dataPlacementRequest();
   }

   private void assertKeyLocation(int cache, Object key) {
      ConsistentHash consistentHash = TestingUtil.extractComponent(cache(cache), DistributionManager.class).getConsistentHash();
      Assert.assertTrue(consistentHash.locateOwners(key).contains(manager(cache).getAddress()), "Cache " + cache +
            " does not contains key " + key + " in consistent hash");
      DataContainer dataContainer = cache(cache).getAdvancedCache().getDataContainer();
      Assert.assertTrue(dataContainer.containsKey(key), "Cache " + cache + " does not contains " + key +
            " in data container");
   }

   private ConfigurationBuilder getConfigurationBuilder() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      builder.dataPlacement().enabled(true).objectLookupFactory(new HashMapObjectLookupFactory());
      builder.clustering().stateTransfer().fetchInMemoryState(true);
      builder.clustering().hash().numOwners(1).numSegments(50)
            .l1().disable();
      builder.customInterceptors().addInterceptor().before(TxInterceptor.class).interceptor(new StreamLibInterceptor());
      return builder;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      addClusterEnabledCacheManager(getConfigurationBuilder());
      addClusterEnabledCacheManager(getConfigurationBuilder());
      waitForClusterToForm();
   }
}
