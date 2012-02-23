package org.infinispan.tx.totalorder.statetransfer;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus <mircea.markus@jboss.com> (C) 2011 Red Hat Inc.
 * @since 5.2
 */
@Test (groups = "functional", testName = "tx.totalorder.statetransfer.DistTotalOrderStateTransferFunctional2PcTest", enabled = false)
public class DistTotalOrderStateTransferFunctional2PcTest extends DistTotalOrderStateTransferFunctional1PcTest {

   
   public DistTotalOrderStateTransferFunctional2PcTest() {
      super(CacheMode.REPL_SYNC, true, true);
   }

}
