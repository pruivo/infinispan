package org.infinispan.counter;

import static org.infinispan.counter.EmbeddedCounterManagerFactory.asCounterManager;
import static org.infinispan.test.TestingUtil.tmpDirectory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterType;
import org.infinispan.counter.api.Storage;
import org.infinispan.counter.configuration.CounterManagerConfigurationBuilder;
import org.infinispan.counter.configuration.Reliability;
import org.infinispan.counter.impl.BaseCounterTest;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestResourceTracker;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * //TODO document this!
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
@Test(groups = "functional", testName = "counter.ConcurrentStartCounterTest")
public class ConcurrentStartCounterTest extends BaseCounterTest {

   private static final String PERSISTENT_FOLDER = tmpDirectory(ConcurrentStartCounterTest.class.getSimpleName());
   private static final String TEMP_PERSISTENT_FOLDER = PERSISTENT_FOLDER + File.separator + "temp";
   private static final int CLUSTER_SIZE = 8;
   private static final long INITIAL_VALUE = 10;
   private static final String COUNTER_NAME = "concurrent-start-counter";

   private static void awaitFuture(Future<Void> future) {
      try {
         future.get(30, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new RuntimeException(e);
      } catch (ExecutionException | TimeoutException e) {
         throw new RuntimeException(e);
      }
   }

   private static GlobalConfigurationBuilder builder(int nodeId) {
      GlobalConfigurationBuilder builder = GlobalConfigurationBuilder.defaultClusteredBuilder();
      builder.globalState().enable().persistentLocation(PERSISTENT_FOLDER + File.separator + nodeId)
            .temporaryLocation(TEMP_PERSISTENT_FOLDER + File.separator + nodeId);
      CounterManagerConfigurationBuilder counterBuilder = builder.addModule(CounterManagerConfigurationBuilder.class);
      counterBuilder.reliability(Reliability.CONSISTENT);
      counterBuilder.addStrongCounter().name(COUNTER_NAME).initialValue(INITIAL_VALUE).storage(Storage.PERSISTENT);
      return builder;
   }

   @AfterMethod(alwaysRun = true)
   public void removeFiles() {
      Util.recursiveFileRemove(PERSISTENT_FOLDER);
      Util.recursiveFileRemove(TEMP_PERSISTENT_FOLDER);
   }

   public void testConcurrentStart() throws BrokenBarrierException, InterruptedException {
      final CyclicBarrier barrier = new CyclicBarrier(CLUSTER_SIZE + 1);
      final List<Future<Void>> futureList = new ArrayList<>(CLUSTER_SIZE);
      for (int i = 0; i < CLUSTER_SIZE; ++i) {
         final int nodeId = i;
         futureList.add(fork(() -> startCacheManager(barrier, nodeId)));
      }
      barrier.await();
      barrier.await();
      futureList.forEach(ConcurrentStartCounterTest::awaitFuture);
      waitForCounterCaches();

      AssertJUnit.assertTrue(counterManager(0).isDefined(COUNTER_NAME));
      //StrongCounter counter = counterManager(0).getStrongCounter(COUNTER_NAME);
      //AssertJUnit.assertTrue(counter.sync().getValue() >= INITIAL_VALUE + CLUSTER_SIZE);
   }

   @Override
   protected int clusterSize() {
      return CLUSTER_SIZE;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      //no-op, test will create them!
   }

   private Void startCacheManager(CyclicBarrier barrier, int id) throws BrokenBarrierException, InterruptedException {
      TestResourceTracker.setThreadTestName(this.getTestName());
      try {
         barrier.await();
         EmbeddedCacheManager cacheManager = addClusterEnabledCacheManager(builder(id),
               getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC));
         asCounterManager(cacheManager).defineCounter(COUNTER_NAME, CounterConfiguration.builder(
               CounterType.UNBOUNDED_STRONG).build());
         //log.tracef("Add finished. New value=%s", value);
         return null;
      }finally {
         barrier.await();
      }
   }

}
