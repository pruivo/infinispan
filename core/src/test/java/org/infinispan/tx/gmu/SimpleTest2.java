package org.infinispan.tx.gmu;

import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", testName = "tx.gmu.SimpleTest2")
public class SimpleTest2 extends SimpleTest {

   @Override
   protected int initialClusterSize() {
      return 5;
   }
}
