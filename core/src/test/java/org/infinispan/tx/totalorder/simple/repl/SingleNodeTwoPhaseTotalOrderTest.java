package org.infinispan.tx.totalorder.simple.repl;

import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test (groups = "functional", testName = "tx.totalorder.repl.SingleNodeTwoPhaseTotalOrderTest")
public class SingleNodeTwoPhaseTotalOrderTest extends SingleNodeOnePhaseTotalOrderTest {

   public SingleNodeTwoPhaseTotalOrderTest() {
      super(false);
   }
}
