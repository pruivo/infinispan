/*
 * INESC-ID, Instituto de Engenharia de Sistemas e Computadores Investigação e Desevolvimento em Lisboa
 * Copyright 2013 INESC-ID and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 3.0 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.tx.gmu;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

import javax.transaction.Transaction;
import java.util.Arrays;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", testName = "tx.gmu.DistConsistencyTest3")
public class DistConsistencyTest3 extends AbstractGMUTest {

   public DistConsistencyTest3() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   public void testConcurrentTransactionConsistency() throws Exception {
      assertAtLeastCaches(3);

      final int initialValue = 1000;
      final int numberOfKeys = 6;

      final Object cache0Key0 = newKey(0, Arrays.asList(1, 2));
      final Object cache0Key1 = newKey(0, Arrays.asList(1,2));
      final Object cache1Key0 = newKey(1, Arrays.asList(0,2));
      final Object cache1Key1 = newKey(1, Arrays.asList(0,2));
      final Object cache2Key0 = newKey(2, Arrays.asList(1,0));
      final Object cache2Key1 = newKey(2, Arrays.asList(1,0));

      logKeysUsedInTest("testConcurrentTransactionConsistency", cache0Key0, cache0Key1, cache1Key0, cache1Key1,
                        cache2Key0, cache2Key1);

      assertKeyOwners(cache0Key0, Arrays.asList(0), Arrays.asList(1, 2));
      assertKeyOwners(cache0Key1, Arrays.asList(0), Arrays.asList(1, 2));
      assertKeyOwners(cache1Key0, Arrays.asList(1), Arrays.asList(0, 2));
      assertKeyOwners(cache1Key1, Arrays.asList(1), Arrays.asList(0, 2));
      assertKeyOwners(cache2Key0, Arrays.asList(2), Arrays.asList(1, 0));
      assertKeyOwners(cache2Key1, Arrays.asList(2), Arrays.asList(1, 0));

      final DelayCommit cache1DelayCommit = addDelayCommit(1, -1);
      final DelayCommit cache2DelayCommit = addDelayCommit(2, -1);

      //init all keys with value_1
      tm(0).begin();
      txPut(0, cache0Key0, initialValue, null);
      txPut(0, cache0Key1, initialValue, null);
      txPut(0, cache1Key0, initialValue, null);
      txPut(0, cache1Key1, initialValue, null);
      txPut(0, cache2Key0, initialValue, null);
      txPut(0, cache2Key1, initialValue, null);
      tm(0).commit();

      //one transaction commits in cache 0 (key0 - 100, key1 + 100)
      //global sum is the same
      tm(0).begin();
      int value = (Integer) cache(0).get(cache0Key0);
      txPut(0, cache0Key0, value - 100, value);
      value = (Integer) cache(0).get(cache0Key1);
      txPut(0, cache0Key1, value + 100, value);
      tm(0).commit();

      int[] allValues = new int[numberOfKeys];
      //read only transaction starts
      tm(0).begin();
      //read from local. value=900 [2,-,-]
      allValues[0] = (Integer) cache(0).get(cache0Key0);
      Transaction readOnlyTx = tm(0).suspend();

      //first concurrent transaction starts and prepares in cache 0 and cache 2 (coord)
      tm(2).begin();
      value = (Integer) cache(2).get(cache2Key0);
      txPut(2, cache2Key0, value - 200, value);
      value = (Integer) cache(2).get(cache0Key0);
      txPut(2, cache0Key0, value + 200, value);
      Transaction concurrentTx1 = tm(2).suspend();
      Thread threadTx1 = prepareInAllNodes(concurrentTx1, cache2DelayCommit, 2);

      //second concurrent transaction stats and prepares in cache 1 (coord) and cache 2
      tm(1).begin();
      value = (Integer) cache(1).get(cache1Key1);
      txPut(1, cache1Key1, value - 300, value);
      value = (Integer) cache(1).get(cache2Key1);
      txPut(1, cache2Key1, value + 300, value);
      Transaction concurrentTx2 = tm(1).suspend();
      Thread threadTx2 = prepareInAllNodes(concurrentTx2, cache1DelayCommit, 1);

      //all transactions are prepared. Commit first transaction first, and then the second one
      cache2DelayCommit.unblock();
      cache1DelayCommit.unblock();
      threadTx1.join();
      threadTx2.join();

      //all transactions are committed. Check if the read only can read a consistent snapshot
      tm(0).resume(readOnlyTx);
      //read from cache 1. first time. can read the most recent. value=700 [2,3,-] 
      allValues[1] = (Integer) cache(0).get(cache1Key1);
      //read form cache 0. value=1100 [2,3,-]
      allValues[2] = (Integer) cache(0).get(cache0Key1);
      //read from cache 1. value=1000 [2,3,-]
      allValues[3] = (Integer) cache(0).get(cache1Key0);
      //read from cache 2. first time. value=1300 [2,3,3]
      allValues[4] = (Integer) cache(0).get(cache2Key1);
      //read from cache 2. value=1000
      allValues[5] = (Integer) cache(0).get(cache2Key0);
      tm(0).commit();

      /*
      cache0Key0->1000(v1)->900(v2*)  ->700(v3:1*)
      cache0Key1->1000(v1)->1100(v2*)
      cache1Key0->1000(v1*)
      cache1Key1->1000(v1)->700(v3:0*)
      cache2Key0->1000(v1*)->1200(v3:1)
      cache2Key1->1000(v1)->1300(v3:0*)

      the * represents the correct versions to read
       */

      int sum = 0;
      for (int v : allValues) {
         sum += v;
      }

      assert sum == (initialValue * numberOfKeys) : "Read an inconsistent snapshot";

      printDataContainer();
      assertNoTransactions();
   }

   public void testConcurrentTransactionConsistency2() throws Exception {
      assertAtLeastCaches(3);

      final int initialValue = 1000;
      final int numberOfKeys = 4;

      final Object cache0Key0 = newKey(0, Arrays.asList(1,2));
      final Object cache0Key1 = newKey(0, Arrays.asList(1,2));
      final Object cache1Key0 = newKey(1, Arrays.asList(0,2));
      final Object cache2Key0 = newKey(2, Arrays.asList(1,0));

      logKeysUsedInTest("testConcurrentTransactionConsistency", cache0Key0, cache0Key1, cache1Key0, cache2Key0);

      assertKeyOwners(cache0Key0, Arrays.asList(0), Arrays.asList(1,2));
      assertKeyOwners(cache0Key1, Arrays.asList(0), Arrays.asList(1,2));
      assertKeyOwners(cache1Key0, Arrays.asList(1), Arrays.asList(0,2));
      assertKeyOwners(cache2Key0, Arrays.asList(2), Arrays.asList(1,0));

      final DelayCommit cache1DelayCommit = addDelayCommit(1, -1);
      final DelayCommit cache2DelayCommit = addDelayCommit(2, -1);

      //init all keys with value_1
      tm(0).begin();
      txPut(0, cache0Key0, initialValue, null);
      txPut(0, cache0Key1, initialValue, null);
      txPut(0, cache1Key0, initialValue, null);
      txPut(0, cache2Key0, initialValue, null);
      tm(0).commit();

      int[] allValues = new int[numberOfKeys];
      //read only transaction starts
      tm(1).begin();
      allValues[0] = (Integer) cache(1).get(cache2Key0);
      //vector [-,-,1]
      Transaction readOnlyTx = tm(1).suspend();

      //first concurrent transaction starts and prepares in cache 0 and cache 2 (coord)
      tm(2).begin();
      int value = (Integer) cache(2).get(cache2Key0);
      txPut(2, cache2Key0, value - 200, value);
      value = (Integer) cache(2).get(cache0Key0);
      txPut(2, cache0Key0, value + 200, value);
      Transaction concurrentTx1 = tm(2).suspend();
      //transaction is prepared with [2,1,1] and [1,1,2]
      Thread threadTx1 = prepareInAllNodes(concurrentTx1, cache2DelayCommit, 2);

      //second concurrent transaction stats and prepares in cache 1 (coord) and cache 0
      tm(1).begin();
      value = (Integer) cache(1).get(cache1Key0);
      txPut(1, cache1Key0, value - 300, value);
      value = (Integer) cache(1).get(cache0Key1);
      txPut(1, cache0Key1, value + 300, value);
      Transaction concurrentTx2 = tm(1).suspend();
      //transaction is prepared with [3,1,1 and [1,2,1]
      Thread threadTx2 = prepareInAllNodes(concurrentTx2, cache1DelayCommit, 1);

      //all transactions are prepared. Commit first transaction first, and then the second one
      cache2DelayCommit.unblock();
      cache1DelayCommit.unblock();
      threadTx1.join();
      threadTx2.join();

      //all transactions are committed. Check if the read only can read a consistent snapshot
      tm(1).resume(readOnlyTx);
      //read first local key to update transaction version to [3,3,1]
      allValues[1] = (Integer) cache(1).get(cache1Key0);
      allValues[2] = (Integer) cache(1).get(cache0Key0);
      allValues[3] = (Integer) cache(1).get(cache0Key1);
      tm(1).commit();

      /*
      cache0Key0->1000(v1*)->1200(v2)
      cache0Key1->1000(v1) ->1300(v3*)
      cache1Key0->1000(v1) ->700(v3*)
      cache2Key0->1000(v1*)->800(v2)

      the * represents the correct versions to read
       */

      int sum = 0;
      for (int v : allValues) {
         sum += v;
      }

      assert sum == (initialValue * numberOfKeys) : "Read an inconsistent snapshot";

      printDataContainer();
      assertNoTransactions();
   }

   public void testWriteConsistency() throws Exception{
      assertAtLeastCaches(2);

      final int initialValue = 1000;
      final int numberOfKeys = 5;

      final Object cache0Key0 = newKey(0, Arrays.asList(1));
      final Object cache0Key1 = newKey(0, Arrays.asList(1));
      final Object cache0Key2 = newKey(0, Arrays.asList(1));
      final Object cache1Key0 = newKey(1, Arrays.asList(0));
      final Object cache1Key1 = newKey(1, Arrays.asList(0));

      logKeysUsedInTest("testConcurrentTransactionConsistency", cache0Key0, cache0Key1, cache0Key2, cache1Key0,
                        cache1Key1);

      assertKeyOwners(cache0Key0, Arrays.asList(0), Arrays.asList(1));
      assertKeyOwners(cache0Key1, Arrays.asList(0), Arrays.asList(1));
      assertKeyOwners(cache0Key2, Arrays.asList(0), Arrays.asList(1));
      assertKeyOwners(cache1Key0, Arrays.asList(1), Arrays.asList(0));
      assertKeyOwners(cache1Key1, Arrays.asList(1), Arrays.asList(0));


      final DelayCommit cache0DelayCommit = addDelayCommit(0, -1);
      final DelayCommit cache1DelayCommit = addDelayCommit(1, -1);

      //init all keys with value_1
      tm(0).begin();
      txPut(0, cache0Key0, initialValue, null);
      txPut(0, cache0Key1, initialValue, null);
      txPut(0, cache0Key2, initialValue, null);
      txPut(0, cache1Key0, initialValue, null);
      txPut(0, cache1Key1, initialValue, null);
      tm(0).commit();

      //first concurrent transaction starts and prepares in cache 0 (coord) and cache 1
      tm(1).begin();
      int value = (Integer) cache(1).get(cache0Key0);
      txPut(1, cache0Key0, value - 200, value);
      value = (Integer) cache(1).get(cache1Key0);
      txPut(1, cache1Key0, value + 200, value);
      Transaction concurrentTx1 = tm(1).suspend();
      //transaction is prepared with [2,1,1] and [1,1,2]
      Thread threadTx1 = prepareInAllNodes(concurrentTx1, cache1DelayCommit, 1);

      //second concurrent transaction stats and prepares in cache 0 (coord) and cache 1
      tm(0).begin();
      value = (Integer) cache(0).get(cache0Key1);
      txPut(0, cache0Key1, value - 300, value);
      value = (Integer) cache(0).get(cache0Key2);
      txPut(0, cache0Key2, value + 300, value);
      Transaction concurrentTx2 = tm(0).suspend();
      Thread threadTx2 = prepareInAllNodes(concurrentTx2, cache0DelayCommit, 0);

      //all transactions are prepared. Commit first transaction first, and then the second one
      cache0DelayCommit.unblock();
      cache1DelayCommit.unblock();
      threadTx1.join();
      threadTx2.join();

      //Problem: sometimes, the transaction reads an old version and does not aborts
      tm(0).begin();
      value = (Integer) cache(0).get(cache0Key0);
      txPut(0, cache0Key0, value - 500, value);
      value = (Integer) cache(0).get(cache1Key1);
      txPut(0, cache1Key1, value + 500, value);
      tm(0).commit();

      int[] allValues = new int[numberOfKeys];
      tm(0).begin();
      allValues[0] = (Integer) cache(0).get(cache0Key0);
      allValues[1] = (Integer) cache(0).get(cache0Key1);
      allValues[2] = (Integer) cache(0).get(cache0Key2);
      allValues[3] = (Integer) cache(0).get(cache1Key0);
      allValues[4] = (Integer) cache(0).get(cache1Key1);
      tm(0).commit();

      int sum = 0;
      for (int v : allValues) {
         sum += v;
      }

      assert sum == (initialValue * numberOfKeys) : "Read an inconsistent snapshot";

      printDataContainer();
      assertNoTransactions();
   }

   @Override
   protected void decorate(ConfigurationBuilder builder) {
      builder.clustering().clustering().hash().numOwners(1);
   }

   @Override
   protected int initialClusterSize() {
      return 3;
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
