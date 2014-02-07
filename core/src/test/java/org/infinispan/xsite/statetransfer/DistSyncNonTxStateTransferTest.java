package org.infinispan.xsite.statetransfer;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "xsite", testName = "xsite.stateTransfer.DistSyncNonTxStateTransferTest")
public class DistSyncNonTxStateTransferTest extends BaseStateTransferTest {

   public DistSyncNonTxStateTransferTest() {
      super();
      implicitBackupCache = true;
   }

   @Override
   protected ConfigurationBuilder getNycActiveConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
   }

   @Override
   protected ConfigurationBuilder getLonActiveConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
   }
}
