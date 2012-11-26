package org.infinispan.tx.gmu;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", testName = "tx.gmu.JoinNodeTest")
public class JoinNodeTest extends AbstractGMUTest {

   private ConfigurationBuilder builder;

   public void testJoinNode() throws Exception {
      assertAtLeastCaches(2);
      assertCacheValuesNull(KEY_1, KEY_2);

      put(0, KEY_1, VALUE_1, null);
      put(0, KEY_2, VALUE_1, null);

      addClusterEnabledCacheManager(builder);
      waitForClusterToForm();
      assertAtLeastCaches(3);

      tm(2).begin();
      cache(2).put(KEY_1, VALUE_2);
      cache(2).put(KEY_2, VALUE_2);
      tm(2).commit();

      assertNoTransactions();
   }

   @Override
   protected void decorate(ConfigurationBuilder builder) {
      //disable state transfer
      builder.clustering().stateTransfer().fetchInMemoryState(false)
            .hash().numOwners(1);
      this.builder = builder;
   }

   @Override
   protected int initialClusterSize() {
      return 2;
   }

   @Override
   protected boolean syncCommitPhase() {
      return true;
   }

   @Override
   protected CacheMode cacheMode() {
      return CacheMode.DIST_SYNC;
   }
}
