package org.infinispan.tx.gmu;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", testName = "tx.gmu.DistConsistencyTest2")
public class DistConsistencyTest2 extends DistConsistencyTest {

   @Override
   protected int initialClusterSize() {
      return 5;
   }

   @Override
   protected void decorate(ConfigurationBuilder builder) {
      builder.clustering().hash().numOwners(2);
   }
}
