package org.infinispan.tx.gmu.statetransfer;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 5.3
 */
@Test(groups = "functional", testName = "tx.gmu.statetransfer.ReplConsistencyTest")
public class ReplConsistencyTest extends BaseConsistencyTest {

   public ReplConsistencyTest() {
      super(4, CacheMode.REPL_SYNC);
   }
}
