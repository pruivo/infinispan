package org.infinispan.lucene.cachestore;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.lucene.directory.DirectoryBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Future;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "stress", testName = "lucene.cachestore.CacheStoreStressTest", singleThreaded = true)
@CleanupAfterMethod
public class CacheStoreStressTest extends SingleCacheManagerTest {

   private static final String TMP_DIR = TestingUtil.tmpDirectory("CacheStoreStressTest");
   private static final String LOCATION = TMP_DIR + File.separator + "stress";
   private static final String METADATA_CACHE_NAME = "index-metadata";
   private static final String LOCK_CACHE_NAME = "index-lock";
   private static final String DATA_CACHE_NAME = "index-data";
   private static final String INDEX_NAME = "test-index";
   private static final int TOTAL_FILES = 10000;

   public void testWithCacheStore() throws Exception {
      final String location = LOCATION + File.separator + "testWithCacheStore";
      performTest(new Configure() {
         @Override
         public void defineConfiguration(EmbeddedCacheManager cacheManager) {
            cacheManager.defineConfiguration(METADATA_CACHE_NAME,
                                             getConfiguration(location, false, false).build());
            cacheManager.defineConfiguration(LOCK_CACHE_NAME,
                                             getConfiguration(location, false, false).build());
            cacheManager.defineConfiguration(DATA_CACHE_NAME,
                                             getConfiguration(location, false, false).build());
         }
      });
   }

   public void testWithCacheStoreAndPassivation() throws Exception {
      final String location = LOCATION + File.separator + "testWithCacheStoreAndPassivation";

      performTest(new Configure() {
         @Override
         public void defineConfiguration(EmbeddedCacheManager cacheManager) {
            cacheManager.defineConfiguration(METADATA_CACHE_NAME,
                                             getConfiguration(location, false, false).build());
            cacheManager.defineConfiguration(LOCK_CACHE_NAME,
                                             getConfiguration(location, false, false).build());
            cacheManager.defineConfiguration(DATA_CACHE_NAME,
                                             getConfiguration(location, false, true).build());
         }
      });
   }

   public void testWithCacheStoreAndPreload() throws Exception {
      final String location = LOCATION + File.separator + "testWithCacheStoreAndPreload";

      performTest(new Configure() {
         @Override
         public void defineConfiguration(EmbeddedCacheManager cacheManager) {
            cacheManager.defineConfiguration(METADATA_CACHE_NAME,
                                             getConfiguration(location, true, false).build());
            cacheManager.defineConfiguration(LOCK_CACHE_NAME,
                                             getConfiguration(location, true, false).build());
            cacheManager.defineConfiguration(DATA_CACHE_NAME,
                                             getConfiguration(location, true, false).build());
         }
      });
   }

   public void testWithCacheStoreAndPreloadAndPassivation() throws Exception {
      final String location = LOCATION + File.separator + "testWithCacheStoreAndPreloadAndPassivation";

      performTest(new Configure() {
         @Override
         public void defineConfiguration(EmbeddedCacheManager cacheManager) {
            cacheManager.defineConfiguration(METADATA_CACHE_NAME,
                                             getConfiguration(location, true, false).build());
            cacheManager.defineConfiguration(LOCK_CACHE_NAME,
                                             getConfiguration(location, true, false).build());
            cacheManager.defineConfiguration(DATA_CACHE_NAME,
                                             getConfiguration(location, true, true).build());
         }
      });
   }

   @AfterMethod(alwaysRun = true)
   public void removeFile() {
      TestingUtil.recursiveFileRemove(TMP_DIR);
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.createCacheManager();
   }

   private ConfigurationBuilder getConfiguration(String location, boolean preload, boolean passivation) {
      ConfigurationBuilder builder = getDefaultStandaloneCacheConfig(false);
      builder.persistence()
            .passivation(passivation);
      builder.persistence()
            .addSingleFileStore()
            .preload(preload)
            .location(location)
            .purgeOnStartup(false);
      if (passivation) {
         builder.eviction().strategy(EvictionStrategy.LIRS).maxEntries(10);
      }
      return builder;
   }

   private void performTest(Configure configure) throws Exception {
      configure.defineConfiguration(cacheManager);
      final Directory directory = createDirectory();
      for (int i = 0; i < TOTAL_FILES; ++i) {
         IndexOutput output = directory.createOutput(fileName(i));
         output.writeInt(i);
         output.close();
      }

      for (int i = 0; i < TOTAL_FILES; ++i) {
         IndexInput indexInput = directory.openInput(fileName(i));
         AssertJUnit.assertEquals("Wrong data stored", i, indexInput.readInt());
         indexInput.close();
      }

      final CyclicBarrier barrier = new CyclicBarrier(4);
      //from TOTAL_FILES to 2*TOTAL_FILE, add even file names...
      Future<Void> adder1 = fork(new AddFileCallable(TOTAL_FILES, 4, barrier, directory));
      Future<Void> adder2 = fork(new AddFileCallable(TOTAL_FILES + 2, 4, barrier, directory));
      //from TOTAL_FILES to zero, remove odd file names
      Future<Void> remover1 = fork(new RemoveFileCallable(TOTAL_FILES - 1, 4, barrier, directory));
      Future<Void> remover2 = fork(new RemoveFileCallable(TOTAL_FILES - 3, 4, barrier, directory));

      adder1.get();
      adder2.get();
      remover1.get();
      remover2.get();

      for (int i = 0; i < 2 * TOTAL_FILES; i += 2) {
         AssertJUnit.assertTrue("Error checking " + fileName(i), directory.fileExists(fileName(i)));
         IndexInput indexInput = directory.openInput(fileName(i));
         AssertJUnit.assertEquals("Wrong data stored", i, indexInput.readInt());
         indexInput.close();
      }

      TestingUtil.killCacheManagers(cacheManager);
      cacheManager = createCacheManager();
      configure.defineConfiguration(cacheManager);

      final Directory directory2 = createDirectory();

      for (int i = 0; i < 2 * TOTAL_FILES; i += 2) {
         AssertJUnit.assertTrue("Error checking " + fileName(i), directory2.fileExists(fileName(i)));
         IndexInput indexInput = directory2.openInput(fileName(i));
         AssertJUnit.assertEquals("Wrong data stored", i, indexInput.readInt());
         indexInput.close();
      }
   }

   private Directory createDirectory() {
      return DirectoryBuilder.newDirectoryInstance(cacheManager.getCache(METADATA_CACHE_NAME),
                                                   cacheManager.getCache(DATA_CACHE_NAME),
                                                   cacheManager.getCache(LOCK_CACHE_NAME),
                                                   INDEX_NAME).create();
   }

   private String fileName(int i) {
      return "FILE_" + i;
   }

   private interface Configure {
      void defineConfiguration(EmbeddedCacheManager cacheManager);
   }

   private class AddFileCallable implements Callable<Void> {

      private final int startIndex;
      private final int step;
      private final CyclicBarrier barrier;
      private final Directory directory;

      private AddFileCallable(int startIndex, int step, CyclicBarrier barrier, Directory directory) {
         this.startIndex = startIndex;
         this.step = step;
         this.barrier = barrier;
         this.directory = directory;
      }

      @Override
      public Void call() throws Exception {
         barrier.await();
         for (int i = startIndex; i < 2 * TOTAL_FILES; i += step) {
            log.debugf("added: %s", fileName(i));
            IndexOutput output = directory.createOutput(fileName(i));
            output.writeInt(i);
            output.close();
         }
         return null;
      }
   }

   private class RemoveFileCallable implements Callable<Void> {

      private final int startIndex;
      private final int step;
      private final CyclicBarrier barrier;
      private final Directory directory;

      private RemoveFileCallable(int startIndex, int step, CyclicBarrier barrier, Directory directory) {
         this.startIndex = startIndex;
         this.step = step;
         this.barrier = barrier;
         this.directory = directory;
      }

      @Override
      public Void call() throws Exception {
         barrier.await();
         for (int i = startIndex; i >= 0; i -= step) {
            log.debugf("removed: %s", fileName(i));
            directory.deleteFile(fileName(i));
         }
         return null;
      }
   }
}
