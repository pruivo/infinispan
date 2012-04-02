package org.infinispan.loaders;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", testName = "loaders.WriteSkewCacheLoaderFunctionalTest")
public class WriteSkewCacheLoaderFunctionalTest extends AbstractInfinispanTest {

   Cache cache;
   CacheStore store;
   TransactionManager tm;
   Configuration cfg;
   EmbeddedCacheManager cm;
   long lifespan = 60000000; // very large lifespan so nothing actually expires

   @BeforeTest
   public void setUp() {
      cfg = new Configuration().fluent()
            .loaders()
            .addCacheLoader(new DummyInMemoryCacheStore.Cfg()
                                  .storeName(this.getClass().getName())) // in order to use the same store
            .transaction().transactionMode(TransactionMode.TRANSACTIONAL)
            .versioning().enable().versioningScheme(VersioningScheme.SIMPLE)
            .locking().isolationLevel(IsolationLevel.REPEATABLE_READ).writeSkewCheck(true)
            .clustering().mode(Configuration.CacheMode.REPL_SYNC).sync()
            .build();
      cm = TestCacheManagerFactory.createCacheManager(cfg);
      cache = cm.getCache();
      store = TestingUtil.extractComponent(cache, CacheLoaderManager.class).getCacheStore();
      tm = TestingUtil.getTransactionManager(cache);
   }

   @AfterTest
   public void tearDown() {
      TestingUtil.killCacheManagers(cm);
      cache = null;
      cm = null;
      cfg = null;
      tm = null;
      store = null;
   }

   @AfterMethod
   public void afterMethod() throws CacheLoaderException {
      if (cache != null) cache.clear();
      if (store != null) store.clear();
   }

   private void assertInCacheAndStore(Object key, Object value) throws CacheLoaderException {
      assertInCacheAndStore(key, value, -1);
   }

   private void assertInCacheAndStore(Object key, Object value, long lifespanMillis) throws CacheLoaderException {
      assertInCacheAndStore(cache, store, key, value, lifespanMillis);
   }


   private void assertInCacheAndStore(Cache cache, CacheStore store, Object key, Object value) throws CacheLoaderException {
      assertInCacheAndStore(cache, store, key, value, -1);
   }

   private void assertInCacheAndStore(Cache cache, CacheStore store, Object key, Object value, long lifespanMillis) throws CacheLoaderException {
      InternalCacheEntry se = cache.getAdvancedCache().getDataContainer().get(key);
      testStoredEntry(se, value, lifespanMillis, "Cache", key);
      se = store.load(key);
      testStoredEntry(se, value, lifespanMillis, "Store", key);
   }

   private void testStoredEntry(InternalCacheEntry entry, Object expectedValue, long expectedLifespan, String src, Object key) {
      assert entry != null : src + " entry for key " + key + " should NOT be null";
      assert entry.getValue().equals(expectedValue) : src + " should contain value " + expectedValue + " under key " + entry.getKey() + " but was " + entry.getValue() + ". Entry is " + entry;
      assert entry.getLifespan() == expectedLifespan : src + " expected lifespan for key " + key + " to be " + expectedLifespan + " but was " + entry.getLifespan() + ". Entry is " + entry;
   }

   private void assertNotInCacheAndStore(Cache cache, CacheStore store, Object... keys) throws CacheLoaderException {
      for (Object key : keys) {
         assert !cache.getAdvancedCache().getDataContainer().containsKey(key) : "Cache should not contain key " + key;
         assert !store.containsKey(key) : "Store should not contain key " + key;
      }
   }

   private void assertNotInCacheAndStore(Object... keys) throws CacheLoaderException {
      assertNotInCacheAndStore(cache, store, keys);
   }

   private void assertInStoreNotInCache(Object... keys) throws CacheLoaderException {
      assertInStoreNotInCache(cache, store, keys);
   }

   private void assertInStoreNotInCache(Cache cache, CacheStore store, Object... keys) throws CacheLoaderException {
      for (Object key : keys) {
         assert !cache.getAdvancedCache().getDataContainer().containsKey(key) : "Cache should not contain key " + key;
         assert store.containsKey(key) : "Store should contain key " + key;
      }
   }

   private void assertInCacheAndNotInStore(Object... keys) throws CacheLoaderException {
      assertInCacheAndNotInStore(cache, store, keys);
   }

   private void assertInCacheAndNotInStore(Cache cache, CacheStore store, Object... keys) throws CacheLoaderException {
      for (Object key : keys) {
         assert cache.getAdvancedCache().getDataContainer().containsKey(key) : "Cache should not contain key " + key;
         assert !store.containsKey(key) : "Store should contain key " + key;
      }
   }

   public void testPreloadingInTransactionalCache() throws Exception {
      Configuration preloadingCfg = cfg.clone();
      preloadingCfg.getCacheLoaderManagerConfig().setPreload(true);
      ((DummyInMemoryCacheStore.Cfg) preloadingCfg.getCacheLoaderManagerConfig().getFirstCacheLoaderConfig()).setStoreName("preloadingCache");

      cm.defineConfiguration("preloadingCache", preloadingCfg);
      Cache preloadingCache = cm.getCache("preloadingCache");
      CacheStore preloadingStore = TestingUtil.extractComponent(preloadingCache, CacheLoaderManager.class).getCacheStore();

      assert preloadingCache.getConfiguration().getCacheLoaderManagerConfig().isPreload();

      assertNotInCacheAndStore(preloadingCache, preloadingStore, "k1", "k2", "k3", "k4");

      preloadingCache.put("k1", "v1");
      preloadingCache.put("k2", "v2", lifespan, MILLISECONDS);
      preloadingCache.put("k3", "v3");
      preloadingCache.put("k4", "v4", lifespan, MILLISECONDS);

      for (int i = 1; i < 5; i++) {
         if (i % 2 == 1)
            assertInCacheAndStore(preloadingCache, preloadingStore, "k" + i, "v" + i);
         else
            assertInCacheAndStore(preloadingCache, preloadingStore, "k" + i, "v" + i, lifespan);
      }

      DataContainer c = preloadingCache.getAdvancedCache().getDataContainer();

      assert c.size() == 4;
      preloadingCache.stop();
      assert c.size() == 0;

      preloadingCache.start();
      assert preloadingCache.getConfiguration().getCacheLoaderManagerConfig().isPreload();

      c = preloadingCache.getAdvancedCache().getDataContainer();
      assert c.size() == 4;

      for (int i = 1; i < 5; i++) {
         if (i % 2 == 1)
            assertInCacheAndStore(preloadingCache, preloadingStore, "k" + i, "v" + i);
         else
            assertInCacheAndStore(preloadingCache, preloadingStore, "k" + i, "v" + i, lifespan);
      }

      TransactionManager transactionManager = preloadingCache.getAdvancedCache().getTransactionManager();

      transactionManager.begin();
      assert preloadingCache.get("k1") == "v1" : "k1 value is different from v1";
      preloadingCache.put("k1", "new-v1");
      transactionManager.commit();
   }
}
