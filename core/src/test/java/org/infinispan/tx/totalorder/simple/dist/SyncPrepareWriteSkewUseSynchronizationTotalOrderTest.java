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
package org.infinispan.tx.totalorder.simple.dist;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.interceptors.locking.OptimisticLockingInterceptor;
import org.infinispan.interceptors.locking.PessimisticLockingInterceptor;
import org.infinispan.interceptors.totalorder.TotalOrderInterceptor;
import org.infinispan.interceptors.totalorder.TotalOrderVersionedDistributionInterceptor;
import org.infinispan.interceptors.totalorder.TotalOrderVersionedEntryWrappingInterceptor;
import org.testng.annotations.Test;

import static junit.framework.Assert.assertTrue;
import static org.testng.Assert.assertFalse;

/**
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", testName = "tx.totalorder.simple.dist.SyncPrepareWriteSkewUseSynchronizationTotalOrderTest")
public class SyncPrepareWriteSkewUseSynchronizationTotalOrderTest extends FullAsyncTotalOrderTest {

   public SyncPrepareWriteSkewUseSynchronizationTotalOrderTest() {
      super(4, CacheMode.DIST_SYNC, false, true, true);
   }

   @Override
   public void testSinglePhaseTotalOrder() {
      assertFalse(Configurations.isOnePhaseTotalOrderCommit(cache(0).getCacheConfiguration()));
   }

   @Override
   public void testInteceptorChain() {
      InterceptorChain ic = advancedCache(0).getComponentRegistry().getComponent(InterceptorChain.class);
      assertTrue(ic.containsInterceptorType(TotalOrderInterceptor.class));
      assertTrue(ic.containsInterceptorType(TotalOrderVersionedDistributionInterceptor.class));
      assertTrue(ic.containsInterceptorType(TotalOrderVersionedEntryWrappingInterceptor.class));
      assertFalse(ic.containsInterceptorType(OptimisticLockingInterceptor.class));
      assertFalse(ic.containsInterceptorType(PessimisticLockingInterceptor.class));
   }
}
