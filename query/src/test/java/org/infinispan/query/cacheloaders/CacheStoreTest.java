package org.infinispan.query.cacheloaders;

import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.TermQuery;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.test.Person;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import java.io.File;
import java.util.List;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

/**
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "query.cacheloaders.CacheStoreTest")
@CleanupAfterMethod
public class CacheStoreTest extends SingleCacheManagerTest {

   private static final String TMP_DIR = TestingUtil.tmpDirectory("query-cache-store-test");
   private static final String LOCATION = TMP_DIR + File.separator + "query";
   private static final String METADATA_CACHE_NAME = "index-metadata";
   private static final String LOCK_CACHE_NAME = "index-lock";
   private static final String DATA_CACHE_NAME = "index-data";
   private static final int NUMBER_PERSONS = 100;

   public void testRestart() throws Exception {
      doTest(false, false, false, "0");
   }

   public void testRestartWithPassivation() throws Exception {
      doTest(true, false, false, "1");
   }

   public void testRestartWithPassivationAndPreload() throws Exception {
      doTest(true, true, false, "2");
   }

   public void testRestartWithPassivationAndEviction() throws Exception {
      doTest(true, false, true, "3");
   }

   public void testRestartWithPreload() throws Exception {
      doTest(false, true, false, "4");
   }

   public void testRestartWithPreloadAndEviction() throws Exception {
      doTest(false, true, true, "5");
   }

   public void testRestartWithEviction() throws Exception {
      doTest(false, false, true, "6");
   }

   public void testRestartWithAll() throws Exception {
      doTest(true, true, true, "7");
   }

   @AfterTest(alwaysRun = true)
   public void removeFolder() {
      TestingUtil.recursiveFileRemove(TMP_DIR);
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.createCacheManager();
   }

   private void doTest(boolean passivation, boolean preload, boolean eviction, String testName) throws Exception {
      defineDirectoryCaches(passivation, preload, eviction, testName);
      Cache cache1 = getIndexingCache(testName);

      putData(cache1);
      //search(cache1);

      cache1.stop();
      TestingUtil.killCacheManagers(cacheManager);

      //System.out.println("Sleeping before restart");
      //Thread.sleep(60000);

      cacheManager = createCacheManager();
      defineDirectoryCaches(passivation, preload, eviction, testName);
      cache1 = getIndexingCache(testName);
      search(cache1);
   }

   private void putData(Cache cache1) {
      for (int i = 0; i < NUMBER_PERSONS; ++i) {
         cache1.put("P" + i, new Person("RandomName" + i, "foobar", i));
      }
   }

   private void search(Cache cache1) throws ParseException {
      for (int i = 0; i < NUMBER_PERSONS; ++i) {
         List list = searchByName("RandomName" + i, cache1);
         log.debugf("Search list: %s", list);
         assertNotNull(list);
         assertEquals(1, list.size());
      }
   }

   private List searchByName(String name, Cache c) throws ParseException {
      SearchManager sm = Search.getSearchManager(c);
      BooleanQuery query = new BooleanQuery();
      query.add(new TermQuery(
            new Term("name", name.toLowerCase())), BooleanClause.Occur.MUST);
      CacheQuery q = sm.getQuery(query, Person.class);
      int resultSize = q.getResultSize();
      List l = q.list();
      assertNotNull(l);
      assertEquals(resultSize, l.size());
      return l;
   }

   private Cache getIndexingCache(Object testName) {
      ConfigurationBuilder builder = getDefaultStandaloneCacheConfig(false);
      builder.persistence()
            .addSingleFileStore()
            .location(LOCATION + File.separator + "d-" + testName)
            .purgeOnStartup(false);
      builder.indexing()
            .indexLocalOnly(true)
            .enable()
            .addProperty("default.directory_provider", "infinispan")
            .addProperty("default.indexmanager", "org.infinispan.query.indexmanager.InfinispanIndexManager")
            .addProperty("infinispan.cachemanager_jndiname", "DefaultCacheManager")
            .addProperty("default.locking_cachename", LOCK_CACHE_NAME)
            .addProperty("default.data_cachename", DATA_CACHE_NAME)
            .addProperty("default.metadata_cachename", METADATA_CACHE_NAME)
            .addProperty("lucene_version", "LUCENE_CURRENT");
      cacheManager.defineConfiguration("index-cache", builder.build());
      return cacheManager.getCache("index-cache");
   }

   private void defineDirectoryCaches(boolean passivation, boolean preload, boolean eviction, String testName) {
      ConfigurationBuilder builder = getDefaultStandaloneCacheConfig(false);
      builder.persistence()
            .passivation(passivation);
      builder.persistence()
            .addSingleFileStore()
            .preload(preload)
            .location(LOCATION + File.separator + testName)
            .purgeOnStartup(false);
      if (eviction) {
         builder.eviction().strategy(EvictionStrategy.LIRS).maxEntries(10);
      } else {
         builder.eviction().strategy(EvictionStrategy.NONE).maxEntries(-1);
      }
      cacheManager.defineConfiguration(DATA_CACHE_NAME, builder.build());
      builder.eviction().strategy(EvictionStrategy.NONE).maxEntries(-1);
      cacheManager.defineConfiguration(LOCK_CACHE_NAME, builder.build());
      builder.persistence()
            .clearStores()
            .addSingleFileStore()
            .preload(true)
            .location(LOCATION + File.separator + testName)
            .purgeOnStartup(false);
      cacheManager.defineConfiguration(METADATA_CACHE_NAME, builder.build());

   }


}
