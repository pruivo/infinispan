/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.tx.gmu.statetransfer;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 5.3
 */
@Test(groups = "functional")
public abstract class BaseSimpleStateTransferTest extends MultipleCacheManagersTest {

   private static final int NUM_KEYS = 100;
   private final int clusterSize;
   private final CacheMode cacheMode;

   protected BaseSimpleStateTransferTest(int clusterSize, CacheMode cacheMode) {
      this.clusterSize = clusterSize;
      this.cacheMode = cacheMode;
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   public void testJoinConsistency() {
      //init cache with some keys
      addKeys(cache(0), 0);
      assertKeyValues(0);

      log.info("Adding a new node...");
      addClusterEnabledCacheManager(createConfiguration());
      log.info("New node added!");

      //assert initial values
      assertKeyValues(0);

      //put more keys
      addKeys(cache(1), 1);

      //assert second values
      assertKeyValues(1);

      assertNoTransactions();
   }

   public void testLeaveConsistency() {
      //init cache with some keys
      addKeys(cache(0), 0);
      assertKeyValues(0);

      log.info("Killing manager 0...");
      TestingUtil.killCacheManagers(manager(0));
      cacheManagers.remove(manager(0));
      log.info("Manager 0 killed!");

      //assert initial values
      assertKeyValues(0);

      //put more keys
      addKeys(cache(1), 1);

      //assert second values
      assertKeyValues(1);

      assertNoTransactions();
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = createConfiguration();
      createCluster(builder, clusterSize);
      waitForClusterToForm();
   }

   protected abstract void decorate(ConfigurationBuilder builder);

   protected final ConfigurationBuilder createConfiguration() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(cacheMode, true);
      builder.locking().lockAcquisitionTimeout(100).isolationLevel(IsolationLevel.SERIALIZABLE)
            .clustering().stateTransfer().fetchInMemoryState(true).awaitInitialTransfer(true)
            .hash().numOwners(3).numSegments(50)
            .versioning().enable().scheme(VersioningScheme.GMU);
      decorate(builder);
      return builder;
   }

   private void assertKeyValues(int iteration) {
      for (Cache cache : caches()) {
         for (int i = 0; i < NUM_KEYS; ++i) {
            Assert.assertEquals(cache.get(getKey(i)), getValue(iteration, i), "Wrong value for key " + getKey(i) + " in cache " + address(cache));
         }
      }
   }

   private void addKeys(Cache<Object, Object> cache, int iteration) {
      for (int i = 0; i < NUM_KEYS; ++i) {
         cache.put(getKey(i), getValue(iteration, i));
      }
   }

   private Object getKey(int i) {
      return "KEY_" + i;
   }

   private Object getValue(int i, int j) {
      return "INIT_" + i + "_" + j;
   }
}
