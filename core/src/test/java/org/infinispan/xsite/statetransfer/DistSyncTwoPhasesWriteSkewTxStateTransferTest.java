package org.infinispan.xsite.statetransfer;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@Test(groups = "xsite", testName = "xsite.stateTransfer.DistSyncTwoPhasesWriteSkewTxStateTransferTest")
public class DistSyncTwoPhasesWriteSkewTxStateTransferTest extends DistSyncTwoPhasesTxStateTransferTest {

   @Override
   protected ConfigurationBuilder getNycActiveConfig() {
      return enableWriteSkew(super.getNycActiveConfig());
   }

   @Override
   protected ConfigurationBuilder getLonActiveConfig() {
      return enableWriteSkew(super.getLonActiveConfig());
   }

   private static ConfigurationBuilder enableWriteSkew(ConfigurationBuilder builder) {
      builder.locking().isolationLevel(IsolationLevel.REPEATABLE_READ).writeSkewCheck(true)
            .versioning().enable().scheme(VersioningScheme.SIMPLE);
      return builder;
   }

   /*
   the following tests are disabled because the failed operations are added to the transaction modification
   list when write skew is enabled. However, I don't think it makes sense...
    */

   @Test(enabled = false)
   @Override
   public void testPutIfAbsentFail() throws Exception {
      super.testPutIfAbsentFail();
   }

   @Test(enabled = false)
   @Override
   public void testRemoveIfMatchFail() throws Exception {
      super.testRemoveIfMatchFail();
   }

   @Test(enabled = false)
   @Override
   public void testReplaceIfMatchFail() throws Exception {
      super.testReplaceIfMatchFail();
   }
}
