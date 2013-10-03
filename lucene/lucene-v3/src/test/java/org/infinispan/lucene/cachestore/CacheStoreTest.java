package org.infinispan.lucene.cachestore;

import org.apache.lucene.store.Directory;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.lucene.directory.DirectoryBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import java.io.File;

import static org.infinispan.lucene.cachestore.TestHelper.createIndex;
import static org.infinispan.lucene.cachestore.TestHelper.verifyOnDirectory;

/**
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "lucene.cachestore.CacheStoreTest")
@CleanupAfterMethod
public class CacheStoreTest extends SingleCacheManagerTest {

   private static final String TMP_DIR = TestingUtil.tmpDirectory("lucene-cache-store-test");
   private static final String LOCATION = TMP_DIR + File.separator + "lucene";
   private static final String METADATA_CACHE_NAME = "index-metadata";
   private static final String LOCK_CACHE_NAME = "index-lock";
   private static final String DATA_CACHE_NAME = "index-data";
   private static final String INDEX_NAME = "test-index";
   private static final int TERMS_TO_ADD = 20000;

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
      Directory directory = createDirectory();
      createIndex(directory, TERMS_TO_ADD, true);
      verifyOnDirectory(directory, TERMS_TO_ADD, true);

      TestingUtil.killCacheManagers(cacheManager);

      cacheManager = createCacheManager();
      defineDirectoryCaches(passivation, preload, eviction, testName);
      directory = createDirectory();
      verifyOnDirectory(directory, TERMS_TO_ADD, true);
   }

   private void defineDirectoryCaches(boolean passivation, boolean preload, boolean eviction, String testName) {
      cacheManager.defineConfiguration(METADATA_CACHE_NAME, createMetadataConfiguration(passivation, testName));
      cacheManager.defineConfiguration(LOCK_CACHE_NAME, createLockConfiguration(passivation, preload, testName));
      cacheManager.defineConfiguration(DATA_CACHE_NAME, createDataConfiguration(passivation, preload, eviction, testName));
   }

   private Configuration createMetadataConfiguration(boolean passivation, Object testName) {
      ConfigurationBuilder builder = getDefaultStandaloneCacheConfig(false);
      builder.persistence()
            .passivation(passivation)
            .addSingleFileStore()
            .preload(true)
            .location(LOCATION + File.separator + testName)
            .purgeOnStartup(false);
      return builder.build();
   }

   private Configuration createLockConfiguration(boolean passivation, boolean preload, Object testName) {
      ConfigurationBuilder builder = getDefaultStandaloneCacheConfig(false);
      builder.persistence()
            .passivation(passivation)
            .addSingleFileStore()
            .preload(preload)
            .location(LOCATION + File.separator + testName)
            .purgeOnStartup(false);
      return builder.build();
   }

   private Configuration createDataConfiguration(boolean passivation, boolean preload, boolean eviction, Object testName) {
      ConfigurationBuilder builder = getDefaultStandaloneCacheConfig(false);
      builder.persistence()
            .passivation(passivation)
            .addSingleFileStore()
            .preload(preload)
            .location(LOCATION + File.separator + testName)
            .purgeOnStartup(false);
      if (eviction) {
         builder.eviction().strategy(EvictionStrategy.LIRS).maxEntries(1);
      }
      return builder.build();
   }

   private Directory createDirectory() {
      return DirectoryBuilder.newDirectoryInstance(cacheManager.getCache(METADATA_CACHE_NAME),
                                                   cacheManager.getCache(DATA_CACHE_NAME),
                                                   cacheManager.getCache(LOCK_CACHE_NAME),
                                                   INDEX_NAME).create();
   }
}
