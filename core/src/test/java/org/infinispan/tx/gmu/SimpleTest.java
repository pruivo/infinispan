package org.infinispan.tx.gmu;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", testName = "tx.gmu.SimpleTest")
public class SimpleTest extends AbstractGMUTest {

   public void testPut() {
      assertAtLeastCaches(2);
      assertCacheValuesNull(KEY_1, KEY_2, KEY_3);

      put(0, KEY_1, VALUE_1, null);

      Map<Object, Object> map = new HashMap<Object, Object>();
      map.put(KEY_2, VALUE_2);
      map.put(KEY_3, VALUE_3);

      putAll(1, map);

      assertNoTransactions();
      printDataContainer();
   }

   public void testPut2() {
      assertAtLeastCaches(2);
      assertCacheValuesNull(KEY_1);

      put(0, KEY_1, VALUE_1, null);
      put(0, KEY_1, VALUE_2, VALUE_1);
      put(0, KEY_1, VALUE_3, VALUE_2);
      put(0, KEY_1, VALUE_3, VALUE_3);

      assertNoTransactions();
      printDataContainer();
   }

   public void removeTest() {
      assertAtLeastCaches(2);
      assertCacheValuesNull(KEY_1);

      put(1, KEY_1, VALUE_1, null);
      remove(0, KEY_1, VALUE_1);
      put(0, KEY_1, VALUE_2, null);
      remove(1, KEY_1, VALUE_2);

      printDataContainer();
      assertNoTransactions();
   }

   public void testPutIfAbsent() {
      assertAtLeastCaches(2);
      assertCacheValuesNull(KEY_1, KEY_2);

      put(1, KEY_1, VALUE_1, null);
      putIfAbsent(0, KEY_1, VALUE_2, VALUE_1, VALUE_1);
      put(1, KEY_1, VALUE_2, VALUE_1);
      putIfAbsent(0, KEY_1, VALUE_3, VALUE_2, VALUE_2);      
      putIfAbsent(0, KEY_2, VALUE_3, null, VALUE_3);

      printDataContainer();
      assertNoTransactions();
   }

   public void testRemoveIfPresent() {
      assertAtLeastCaches(2);
      assertCacheValuesNull(KEY_1);

      put(0, KEY_1, VALUE_1, null);
      put(1, KEY_1, VALUE_2, VALUE_1);
      removeIf(0, KEY_1, VALUE_1, VALUE_2, false);
      removeIf(0, KEY_1, VALUE_2, null, true);

      printDataContainer();
      assertNoTransactions();
   }

   public void testClear() {
      assertAtLeastCaches(2);
      assertCacheValuesNull(KEY_1);

      put(0, KEY_1, VALUE_1, null);

      cache(0).clear();
      assertCachesValue(0, KEY_1, null);

      printDataContainer();
      assertNoTransactions();
   }

   public void testReplace() {
      assertAtLeastCaches(2);
      assertCacheValuesNull(KEY_1);

      put(1, KEY_1, VALUE_1, null);
      replace(0, KEY_1, VALUE_2, VALUE_1);
      put(0, KEY_1, VALUE_3, VALUE_2);
      replace(0, KEY_1, VALUE_3, VALUE_3);

      printDataContainer();
      assertNoTransactions();
   }

   public void testReplaceWithOldVal() {
      assertAtLeastCaches(2);
      assertCacheValuesNull(KEY_1);

      put(1, KEY_1, VALUE_2, null);
      put(0, KEY_1, VALUE_3, VALUE_2);
      replaceIf(0, KEY_1, VALUE_1, VALUE_2, VALUE_3, false);
      replaceIf(0, KEY_1, VALUE_1, VALUE_3, VALUE_1, true);

      printDataContainer();
      assertNoTransactions();
   }

   public void testRemoveUnexistingEntry() {
      assertAtLeastCaches(1);
      assertCacheValuesNull(KEY_1);

      remove(0, KEY_1, null);

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
