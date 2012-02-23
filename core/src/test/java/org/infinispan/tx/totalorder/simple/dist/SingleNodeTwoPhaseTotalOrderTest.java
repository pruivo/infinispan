package org.infinispan.tx.totalorder.simple.dist;

import org.testng.annotations.Test;

/**
 * A simple single node test for total order based protocol in distributed mode
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", testName = "tx.totalorder.dist.SingleNodeTwoPhaseTotalOrderTest")
public class SingleNodeTwoPhaseTotalOrderTest extends SingleNodeOnePhaseTotalOrderTest {
   public SingleNodeTwoPhaseTotalOrderTest() {
      super(false);
   }
}
