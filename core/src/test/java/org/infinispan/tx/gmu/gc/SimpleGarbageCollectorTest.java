package org.infinispan.tx.gmu.gc;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.container.gmu.GMUDataContainer;
import org.infinispan.transaction.gmu.manager.GarbageCollectorManager;
import org.infinispan.tx.gmu.AbstractGMUTest;
import org.testng.annotations.Test;

import javax.transaction.Transaction;

import static junit.framework.Assert.assertEquals;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", testName = "tx.gmu.gc.SimpleGarbageCollectorTest")
public class SimpleGarbageCollectorTest extends AbstractGMUTest {

   public SimpleGarbageCollectorTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   public void testKeepMostRecent() {
      assertAtLeastCaches(2);
      assertCacheValuesNull(KEY_1);

      GarbageCollectorManager gcManager = getComponent(0, GarbageCollectorManager.class);
      final GMUDataContainer gmuDataContainer = (GMUDataContainer) getComponent(0, DataContainer.class);

      put(0, KEY_1, VALUE_1, null);
      put(0, KEY_1, VALUE_2, VALUE_1);
      put(0, KEY_1, VALUE_3, VALUE_2);

      assertNoTransactions();
      assert gmuDataContainer.getVersionChain(KEY_1).numberOfVersion() == 3 : "Wrong number of versions in version chain";
      //no transactions are running... so the garbage collect should only keep the most recent one
      gcManager.triggerVersionGarbageCollection();
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return gmuDataContainer.getVersionChain(KEY_1).numberOfVersion() == 1;
         }
      });

      assertEquals(VALUE_3, cache(0).get(KEY_1));
      assertEquals(VALUE_3, cache(1).get(KEY_1));

      assertNoTransactions();
   }

   public void testKeepAll() throws Exception {
      assertAtLeastCaches(2);
      assertCacheValuesNull(KEY_1, KEY_2, KEY_3);

      GarbageCollectorManager gcManager = getComponent(0, GarbageCollectorManager.class);
      final GMUDataContainer gmuDataContainer = (GMUDataContainer) getComponent(0, DataContainer.class);

      put(0, KEY_1, VALUE_1, null);
      put(0, KEY_2, VALUE_1, null);
      put(0, KEY_3, VALUE_1, null);

      put(0, KEY_1, VALUE_2, VALUE_1);
      put(0, KEY_2, VALUE_2, VALUE_1);
      put(0, KEY_3, VALUE_2, VALUE_1);

      tm(0).begin();
      assertEquals(VALUE_2, cache(0).get(KEY_1));
      Transaction readOnlyTx = tm(0).suspend();

      put(0, KEY_1, VALUE_3, VALUE_2);
      put(0, KEY_2, VALUE_3, VALUE_2);
      put(0, KEY_3, VALUE_3, VALUE_2);

      put(0, KEY_1, VALUE_1, VALUE_3);
      put(0, KEY_2, VALUE_1, VALUE_3);
      put(0, KEY_3, VALUE_1, VALUE_3);

      assert gmuDataContainer.getVersionChain(KEY_1).numberOfVersion() == 4 : "Wrong Number Of Versions for " + KEY_1;
      assert gmuDataContainer.getVersionChain(KEY_2).numberOfVersion() == 4 : "Wrong Number Of Versions for " + KEY_2;
      assert gmuDataContainer.getVersionChain(KEY_3).numberOfVersion() == 4 : "Wrong Number Of Versions for " + KEY_3;

      gcManager.triggerVersionGarbageCollection();

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return gmuDataContainer.getVersionChain(KEY_1).numberOfVersion() == 3 &&
                  gmuDataContainer.getVersionChain(KEY_2).numberOfVersion() == 3 &&
                  gmuDataContainer.getVersionChain(KEY_3).numberOfVersion() == 3;
         }
      });

      tm(0).resume(readOnlyTx);
      assertEquals(VALUE_2, cache(0).get(KEY_1));
      assertEquals(VALUE_2, cache(0).get(KEY_2));
      assertEquals(VALUE_2, cache(0).get(KEY_3));
      tm(0).commit();

      assertNoTransactions();

      gcManager.triggerVersionGarbageCollection();

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return gmuDataContainer.getVersionChain(KEY_1).numberOfVersion() == 1 &&
                  gmuDataContainer.getVersionChain(KEY_2).numberOfVersion() == 1 &&
                  gmuDataContainer.getVersionChain(KEY_3).numberOfVersion() == 1;
         }
      });

      assertNoTransactions();
   }

   @Override
   protected void decorate(ConfigurationBuilder builder) {
      builder.garbageCollector().enabled(true);
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
      return CacheMode.REPL_SYNC;
   }
}
