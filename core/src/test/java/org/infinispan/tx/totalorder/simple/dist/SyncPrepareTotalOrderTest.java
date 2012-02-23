package org.infinispan.tx.totalorder.simple.dist;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", testName = "tx.totalorder.simple.dist.SyncPrepareTotalOrderTest")
public class SyncPrepareTotalOrderTest extends FullAsyncTotalOrderTest {

   public SyncPrepareTotalOrderTest() {
      super(4, CacheMode.DIST_SYNC, false, false, false);
   }
}
