package org.infinispan.tx;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.dataplacement.DataPlacementManager;
import org.infinispan.dataplacement.hm.HashMapObjectLookupFactory;
import org.infinispan.distribution.DistributionManager;
import org.testng.annotations.Test;

import static org.infinispan.test.TestingUtil.extractComponent;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", testName = "tx.DistSelfTunningTest")
public class DistSelfTunningTest extends AbstractSelfTunningTest {

   public DistSelfTunningTest() {
      super(CacheMode.DIST_SYNC);
      builder.clustering().hash().numOwners(1);
      builder.dataPlacement().enabled(true)
            .objectLookupFactory(new HashMapObjectLookupFactory())
            .coolDownTime(1000);
   }

   public void testReplicationDegree() throws Exception {
      populate();
      DistributionManager cache0DM = extractComponent(cache(0), DistributionManager.class);
      DistributionManager cache1DM = extractComponent(cache(1), DistributionManager.class);
      DataPlacementManager dataPlacementManager = extractComponent(cache(0), DataPlacementManager.class);

      assertReplicationDegree(cache0DM, 1);
      assertReplicationDegree(cache1DM, 1);

      triggerTunningReplicationDegree(dataPlacementManager, 2);

      assertReplicationDegree(cache0DM, 2);
      assertReplicationDegree(cache1DM, 2);

      addClusterEnabledCacheManager(builder);
      waitForClusterToForm();

      assertReplicationDegree(cache0DM, 2);
      assertReplicationDegree(cache1DM, 2);
      assertReplicationDegree(extractComponent(cache(2), DistributionManager.class), 2);
      assertKeysValue();
   }
}
