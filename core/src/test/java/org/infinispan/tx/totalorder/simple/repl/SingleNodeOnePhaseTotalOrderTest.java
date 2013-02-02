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
package org.infinispan.tx.totalorder.simple.repl;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.interceptors.locking.OptimisticLockingInterceptor;
import org.infinispan.interceptors.locking.PessimisticLockingInterceptor;
import org.infinispan.interceptors.totalorder.TotalOrderInterceptor;
import org.infinispan.interceptors.totalorder.TotalOrderVersionedEntryWrappingInterceptor;
import org.infinispan.interceptors.totalorder.TotalOrderVersionedReplicationInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.TransactionProtocol;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static junit.framework.Assert.assertTrue;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;

/**
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test (groups = "functional", testName = "tx.totalorder.repl.SingleNodeOnePhaseTotalOrderTest")
public class SingleNodeOnePhaseTotalOrderTest extends MultipleCacheManagersTest {

   protected static final String KEY_1 = "key_1";
   protected static final String KEY_2 = "key_2";
   protected static final String KEY_3 = "key_3";
   protected static final String VALUE_1 = "value_1";
   protected static final String VALUE_2 = "value_2";
   protected static final String VALUE_3 = "value_3";
   protected static final String VALUE_4 = "value_4";

   protected final boolean onePhaseTotalOrder;

   public SingleNodeOnePhaseTotalOrderTest() {
      this(true);
   }

   protected SingleNodeOnePhaseTotalOrderTest(boolean onePhaseTotalOrder) {
      this.onePhaseTotalOrder = onePhaseTotalOrder;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder dcc = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      dcc.transaction().transactionProtocol(TransactionProtocol.TOTAL_ORDER).syncCommitPhase(true)
            .syncRollbackPhase(true)
            .recovery().disable();
      dcc.locking().isolationLevel(IsolationLevel.REPEATABLE_READ).writeSkewCheck(true);
      dcc.transaction().useSynchronization(onePhaseTotalOrder);
      dcc.versioning().enable().scheme(VersioningScheme.SIMPLE);

      createCluster(dcc, 1);
      waitForClusterToForm();
   }

   public void testInteceptorChain() {
      InterceptorChain ic = advancedCache(0).getComponentRegistry().getComponent(InterceptorChain.class);
      assertTrue(ic.containsInterceptorType(TotalOrderInterceptor.class));
      assertTrue(ic.containsInterceptorType(TotalOrderVersionedReplicationInterceptor.class));
      assertTrue(ic.containsInterceptorType(TotalOrderVersionedEntryWrappingInterceptor.class));
      assertFalse(ic.containsInterceptorType(OptimisticLockingInterceptor.class));
      assertFalse(ic.containsInterceptorType(PessimisticLockingInterceptor.class));
   }

   public void testToCacheIsTransactional() {
      assertTrue(cache(0).getCacheConfiguration().transaction().autoCommit());
      assertTrue(cache(0).getCacheConfiguration().transaction().transactionMode().isTransactional());
   }

   public void testSinglePhaseTotalOrder() {
      if (onePhaseTotalOrder) {
         assertTrue(Configurations.isOnePhaseTotalOrderCommit(cache(0).getCacheConfiguration()));
      } else {
         assertFalse(Configurations.isOnePhaseTotalOrderCommit(cache(0).getCacheConfiguration()));
      }
   }

   public void testPut() {
      assertEmpty(KEY_1, KEY_2, KEY_3);

      cache(0).put(KEY_1, VALUE_1);

      assertCacheValue(KEY_1, VALUE_1);

      Map<Object, Object> map = new HashMap<Object, Object>();
      map.put(KEY_2, VALUE_2);
      map.put(KEY_3, VALUE_3);

      cache(0).putAll(map);

      assertCacheValue(KEY_1, VALUE_1);
      assertCacheValue(KEY_2, VALUE_2);
      assertCacheValue(KEY_3, VALUE_3);

      assertNoTransactions();
   }

   public void removeTest() {
      assertEmpty(KEY_1);

      cache(0).put(KEY_1, VALUE_1);

      assertCacheValue(KEY_1, VALUE_1);

      cache(0).remove(KEY_1);

      assertCacheValue(KEY_1, null);

      cache(0).put(KEY_1, VALUE_1);

      assertCacheValue(KEY_1, VALUE_1);

      cache(0).remove(KEY_1);

      assertCacheValue(KEY_1, null);

      assertNoTransactions();
   }

   public void testPutIfAbsent() {
      assertEmpty(KEY_1, KEY_2);

      cache(0).put(KEY_1, VALUE_1);

      assertCacheValue(KEY_1, VALUE_1);

      cache(0).putIfAbsent(KEY_1, VALUE_2);

      assertCacheValue(KEY_1, VALUE_1);

      cache(0).put(KEY_1, VALUE_3);

      assertCacheValue(KEY_1, VALUE_3);

      cache(0).putIfAbsent(KEY_1, VALUE_4);

      assertCacheValue(KEY_1, VALUE_3);

      cache(0).putIfAbsent(KEY_2, VALUE_1);

      assertCacheValue(KEY_2, VALUE_1);

      assertNoTransactions();
   }

   public void testRemoveIfPresent() {
      assertEmpty(KEY_1);

      cache(0).put(KEY_1, VALUE_1);
      cache(0).put(KEY_1, VALUE_2);

      assertCacheValue(KEY_1, VALUE_2);

      cache(0).remove(KEY_1, VALUE_1);

      assertCacheValue(KEY_1, VALUE_2);

      cache(0).remove(KEY_1, VALUE_2);

      assertCacheValue(KEY_1, null);

      assertNoTransactions();
   }

   public void testClear() {
      assertEmpty(KEY_1);

      cache(0).put(KEY_1, VALUE_1);

      assertCacheValue(KEY_1, VALUE_1);

      cache(0).clear();

      assertCacheValue(KEY_1, null);

      assertNoTransactions();
   }

   public void testReplace() {
      assertEmpty(KEY_1);

      cache(0).put(KEY_1, VALUE_1);

      assertCacheValue(KEY_1, VALUE_1);

      Assert.assertEquals(cache(0).replace(KEY_1, VALUE_2), VALUE_1);

      assertCacheValue(KEY_1, VALUE_2);

      cache(0).put(KEY_1, VALUE_3);

      cache(0).replace(KEY_1, VALUE_3);

      assertCacheValue(KEY_1, VALUE_3);

      assertNoTransactions();
   }

   public void testReplaceWithOldVal() {
      assertEmpty(KEY_1);

      cache(0).put(KEY_1, VALUE_1);

      assertCacheValue(KEY_1, VALUE_1);

      cache(0).put(KEY_1, VALUE_2);

      assertCacheValue(KEY_1, VALUE_2);

      cache(0).replace(KEY_1, VALUE_3, VALUE_4);

      assertCacheValue(KEY_1, VALUE_2);

      cache(0).replace(KEY_1, VALUE_2, VALUE_4);

      assertCacheValue(KEY_1, VALUE_4);

      assertNoTransactions();
   }

   public void testRemoveUnexistingEntry() {
      assertEmpty(KEY_1);

      cache(0).remove(KEY_1);

      assertCacheValue(KEY_1, null);

      assertNoTransactions();
   }

   protected void assertEmpty(Object... keys) {
      for (Cache cache : caches()) {
         for (Object key : keys) {
            assertNull(cache.get(key));
         }
      }
   }

   //originatorIndex == cache which executed the transaction
   protected void assertCacheValue(Object key, Object value) {
      for (int index = 0; index < caches().size(); ++index) {
         assertEquals(index, key, value);
      }

   }

   private void assertEquals(int index, Object key, Object value) {
      assert value == null ? value == cache(index).get(key) : value.equals(cache(index).get(key));
   }
}
