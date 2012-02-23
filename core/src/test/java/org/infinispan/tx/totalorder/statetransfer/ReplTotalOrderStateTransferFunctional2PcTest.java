package org.infinispan.tx.totalorder.statetransfer;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.LegacyConfigurationAdaptor;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.statetransfer.StateTransferFunctionalTest;
import org.infinispan.transaction.TransactionProtocol;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus <mircea.markus@jboss.com> (C) 2011 Red Hat Inc.
 * @since 5.2
 */
@Test (groups = "functional", testName = "tx.totalorder.statetransfer.ReplTotalOrderStateTransferFunctional2PcTest", enabled = false)
public class ReplTotalOrderStateTransferFunctional2PcTest extends ReplTotalOrderStateTransferFunctional1PcTest {

   
   public ReplTotalOrderStateTransferFunctional2PcTest() {
      super(CacheMode.REPL_SYNC, true, true, false);
   }

}
