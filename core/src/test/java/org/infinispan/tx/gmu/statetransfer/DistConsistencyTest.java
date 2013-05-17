package org.infinispan.tx.gmu.statetransfer;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 5.3
 */
@Test(groups = "functional", testName = "tx.gmu.statetransfer.DistConsistencyTest")
public class DistConsistencyTest extends BaseConsistencyTest {

   public DistConsistencyTest() {
      super(4, CacheMode.DIST_SYNC);
   }
}
