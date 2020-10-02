package org.infinispan.xsite;

import static org.infinispan.test.TestingUtil.extractComponent;
import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.replaceComponent;
import static org.infinispan.test.TestingUtil.v;
import static org.testng.AssertJUnit.assertEquals;

import java.lang.reflect.Method;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.expiration.impl.InternalExpirationManager;
import org.infinispan.interceptors.impl.CacheMgmtInterceptor;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.util.ControlledTimeService;
import org.testng.annotations.Test;

/**
 * //TODO document this!
 *
 * @author Pedro Ruivo
 * @since 12.0
 */
@Test(groups = "xsite", testName = "xsite.AnotherXSiteTest")
public class AnotherXSiteTest extends AbstractMultipleSitesTest {

   private static final AtomicInteger GENERATOR = new AtomicInteger();
   private static final String BASE_PATH = CommonsTestingUtil.tmpDirectory(AnotherXSiteTest.class);
   private static final int MAX_IDLE = 1000;
   private static final int NUM_KEYS = 1000;
   private final ControlledTimeService timeService = new ControlledTimeService();

   public void doTest(Method method) {
      log.info("Inserting data");
      for (int i = 0; i < NUM_KEYS; ++i) {
         String key = k(method, i);
         String value = v(method, i);
         cache(0, null, 0).put(key, value);
      }

      log.info("Checking data");
      for (int i = 0; i < NUM_KEYS; ++i) {
         String key = k(method, i);
         String value = v(method, i);
         eventuallyAssertInAllSitesAndCaches(cache -> {
            String v  = String.valueOf(cache.get(key));
            log.infof("%s=%s? %s", key, value, v);
            return Objects.equals(value, v);
         });
      }

      // simulate JMX call
      assertEquals(NUM_KEYS, extractComponent(cache(0, null, 0), CacheMgmtInterceptor.class).getNumberOfEntries());
      assertEquals(NUM_KEYS, extractComponent(cache(1, null, 0), CacheMgmtInterceptor.class).getNumberOfEntries());

      log.info("Expiring all keys");
      timeService.advance(MAX_IDLE + 1);

      log.info("Running the expiration reaper");
      caches(siteName(0)).forEach(cache -> extractComponent(cache, InternalExpirationManager.class).processExpiration());
      caches(siteName(1)).forEach(cache -> extractComponent(cache, InternalExpirationManager.class).processExpiration());

      // simulate JMX call
      assertEquals(0, extractComponent(cache(0, null, 0), CacheMgmtInterceptor.class).getNumberOfEntries());
      assertEquals(0, extractComponent(cache(1, null, 0), CacheMgmtInterceptor.class).getNumberOfEntries());
   }

   @Override
   protected int defaultNumberOfSites() {
      return 2;
   }

   @Override
   protected int defaultNumberOfNodes() {
      return 3;
   }

   @Override
   protected ConfigurationBuilder defaultConfigurationForSite(int siteIndex) {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC);
      builder.sites().addBackup()
             .site(siteName(siteIndex == 0 ? 1 : 0))
             .strategy(BackupConfiguration.BackupStrategy.SYNC);
      builder.memory()
             .storage(StorageType.OFF_HEAP)
             .maxCount(10)
             .whenFull(EvictionStrategy.REMOVE);
      builder.expiration().maxIdle(MAX_IDLE, TimeUnit.MILLISECONDS);
      builder.persistence().passivation(true)
             .addSingleFileStore()
             .maxEntries(1_000_000)
             .shared(false)
             .preload(false)
             .fetchPersistentState(true)
             .purgeOnStartup(true)
             .location(Paths.get(BASE_PATH, "site" + siteIndex, "node" + GENERATOR.incrementAndGet()).toString());
      return builder;
   }

   @Override
   protected void afterSitesCreated() {
      super.afterSitesCreated();
      //use the same time service instance in all nodes
      sites.forEach(
            site -> site.cacheManagers.forEach(cm -> {
               replaceComponent(cm, TimeService.class, timeService, true);
               PersistenceManager persistenceManager = extractComponent(cm.getCache(), PersistenceManager.class);
               persistenceManager.stop();
               persistenceManager.start();
            }));
   }
}
