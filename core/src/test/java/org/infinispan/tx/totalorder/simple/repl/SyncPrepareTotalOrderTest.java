package org.infinispan.tx.totalorder.simple.repl;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", testName = "tx.totalorder.simple.repl.SyncPrepareTotalOrderTest")
public class SyncPrepareTotalOrderTest extends FullAsyncTotalOrderTest {

   public SyncPrepareTotalOrderTest() {
      super(3, CacheMode.REPL_SYNC, false, false, false);
   }
}
