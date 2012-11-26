package org.infinispan.tx.gmu;

import org.testng.annotations.Test;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", testName = "tx.gmu.ConsistencyTest2")
public class ConsistencyTest2 extends ConsistencyTest {

   @Override
   protected int initialClusterSize() {
      return 5;
   }
}
