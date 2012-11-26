package org.infinispan.tx.gmu;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

import javax.transaction.Transaction;

import static junit.framework.Assert.assertEquals;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", testName = "tx.gmu.ConsistencyTest")
public class ConsistencyTest extends AbstractGMUTest {

   public void testGetSnapshot() throws Exception {
      assertAtLeastCaches(2);

      Object cache0Key0 = newKey(0, 1);
      Object cache1Key1 = newKey(1, 0);
      Object cache1Key2 = newKey(1, 0);

      logKeysUsedInTest("testGetSnapshot", cache0Key0, cache1Key1, cache1Key2);

      assertKeyOwners(cache0Key0, 0, 1);
      assertKeyOwners(cache1Key1, 1, 0);
      assertKeyOwners(cache1Key2, 1, 0);
      assertCacheValuesNull(cache0Key0, cache1Key1, cache1Key2);

      tm(0).begin();
      txPut(0, cache0Key0, VALUE_1, null);
      txPut(0, cache1Key1, VALUE_1, null);
      txPut(0, cache1Key2, VALUE_1, null);
      tm(0).commit();

      assertCachesValue(0, cache0Key0, VALUE_1);
      assertCachesValue(0, cache1Key1, VALUE_1);
      assertCachesValue(0, cache1Key2, VALUE_1);

      tm(0).begin();
      assert VALUE_1.equals(cache(0).get(cache0Key0));
      Transaction tx0 = tm(0).suspend();

      tm(1).begin();
      cache(1).put(cache0Key0, VALUE_2);
      cache(1).put(cache1Key1, VALUE_2);
      cache(1).put(cache1Key2, VALUE_2);
      tm(1).commit();

      assertCachesValue(1, cache0Key0, VALUE_2);
      assertCachesValue(1, cache1Key1, VALUE_2);
      assertCachesValue(1, cache1Key2, VALUE_2);

      tm(0).resume(tx0);
      assert VALUE_1.equals(cache(0).get(cache0Key0));
      assert VALUE_1.equals(cache(0).get(cache1Key1));
      assert VALUE_1.equals(cache(0).get(cache1Key2));
      tm(0).commit();

      assertNoTransactions();
      printDataContainer();
   }

   public void testPrematureAbort() throws Exception {
      assertAtLeastCaches(2);

      Object cache0Key = newKey(0, 1);
      Object cache1Key = newKey(1, 0);

      logKeysUsedInTest("testPrematureAbort", cache0Key, cache1Key);

      assertKeyOwners(cache0Key, 0, 1);
      assertKeyOwners(cache1Key, 1, 0);
      assertCacheValuesNull(cache0Key, cache1Key);

      tm(0).begin();
      txPut(0, cache0Key, VALUE_1, null);
      txPut(0, cache1Key, VALUE_1, null);
      tm(0).commit();


      tm(0).begin();
      Object value = cache(0).get(cache0Key);
      assertEquals(VALUE_1, value);
      Transaction tx0 = tm(0).suspend();

      tm(1).begin();
      txPut(1, cache0Key, VALUE_2, VALUE_1);
      txPut(1, cache1Key, VALUE_2, VALUE_1);
      tm(1).commit();

      tm(0).resume(tx0);
      txPut(0, cache0Key, VALUE_3, VALUE_1);
      try{
         cache(0).get(cache1Key);
         assert false : "Expected to abort read write transaction";
      } catch (Exception e) {
         safeRollback(0);
      }

      printDataContainer();
      assertNoTransactions();
   }

   public void testConflictingTxs() throws Exception {
      assertAtLeastCaches(2);

      Object cache0Key = newKey(0, 1);
      Object cache1Key = newKey(1, 0);

      logKeysUsedInTest("testConflictingTxs", cache0Key, cache1Key);

      assertKeyOwners(cache0Key, 0, 1);
      assertKeyOwners(cache1Key, 1, 0);
      assertCacheValuesNull(cache0Key, cache1Key);

      tm(0).begin();
      txPut(0, cache0Key, VALUE_1, null);
      txPut(0, cache1Key, VALUE_1, null);
      tm(0).commit();


      tm(0).begin();
      Object value = cache(0).get(cache0Key);
      assertEquals(VALUE_1, value);
      Transaction tx0 = tm(0).suspend();

      tm(1).begin();
      txPut(1, cache0Key, VALUE_2, VALUE_1);
      txPut(1, cache1Key, VALUE_2, VALUE_1);
      tm(1).commit();

      tm(0).resume(tx0);
      try{
         txPut(0, cache0Key, VALUE_3, VALUE_1);
         txPut(0, cache1Key, VALUE_3, VALUE_1);
         tm(0).commit();
         assert false : "Expected to abort conflicting transaction";
      } catch (Exception e) {
         safeRollback(0);
      }

      printDataContainer();
      assertNoTransactions();
   }



   @Override
   protected void decorate(ConfigurationBuilder builder) {
      //no-op
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
