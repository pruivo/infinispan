package org.infinispan.tx;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", testName = "tx.ReplSelfTunningTest")
public class ReplSelfTunningTest extends AbstractSelfTunningTest {

   public ReplSelfTunningTest() {
      super(CacheMode.REPL_SYNC);
   }
}
