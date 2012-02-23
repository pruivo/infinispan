package org.infinispan.tx.totalorder.simple.dist;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", testName = "tx.totalorder.simple.dist.FullSyncTotalOrderTest")
public class FullSyncTotalOrderTest extends FullAsyncTotalOrderTest {

   public FullSyncTotalOrderTest() {
      this(4);
   }
   
   public FullSyncTotalOrderTest(int clusterSize) {
      super(clusterSize, CacheMode.DIST_SYNC, true, false, false);
   }
}
