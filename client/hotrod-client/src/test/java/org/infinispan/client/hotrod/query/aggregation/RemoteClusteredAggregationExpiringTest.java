package org.infinispan.client.hotrod.query.aggregation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.infinispan.query.aggregation.QueryAggregationExpiringTest.resultMaps;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.test.annotation.TestForIssue;
import org.infinispan.commons.time.ControlledTimeService;
import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.query.model.Task;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.query.aggregation.RemoteClusteredAggregationExpiringTest")
@TestForIssue(githubKey = "13194")
public class RemoteClusteredAggregationExpiringTest extends MultiHotRodServersTest {

   private final ControlledTimeService timeService = new ControlledTimeService();

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder config = hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC));
      config.clustering()
            .hash().numOwners(1);
      config.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity("model.Task");

      createHotRodServers(1, config);
      waitForClusterToForm();
      TestingUtil.replaceComponent(manager(0), TimeService.class, timeService, true);
   }

   @Override
   protected SerializationContextInitializer contextInitializer() {
      return Task.TaskSchema.INSTANCE;
   }

   @Test
   public void test() throws Exception {
      RemoteCache<Integer, Task> remoteCache = client(0).getCache();

      remoteCache.put(1, new Task(101, "type-1", "status-1", "label-1"), 1, TimeUnit.SECONDS);
      remoteCache.put(2, new Task(102, "type-2", "status-2", "label-2"), 3, TimeUnit.SECONDS);

      assertResultSize(remoteCache, 2);

      // expire key=1
      timeService.advance(2, TimeUnit.SECONDS);

      assertResultSize(remoteCache, 1);

      // expire key=2
      timeService.advance(2, TimeUnit.SECONDS);

      assertResultSize(remoteCache, 0);
   }

   private static void assertResultSize(RemoteCache<Integer, Task> cache, int expectedSize) {
      Query<Object[]> query = cache.query("select status, count(status) from model.Task group by status");
      Map<String, Long> result = resultMaps(query.list());
      assertThat(result).size().isEqualTo(expectedSize);

      query = cache.query("select label, count(label) from model.Task group by label");
      result = resultMaps(query.list());
      assertThat(result).size().isEqualTo(expectedSize);

      query = cache.query("select type, count(type) from model.Task group by type");
      result = resultMaps(query.list());
      assertThat(result).size().isEqualTo(expectedSize);
   }
}
