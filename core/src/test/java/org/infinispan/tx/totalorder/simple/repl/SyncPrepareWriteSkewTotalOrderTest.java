package org.infinispan.tx.totalorder.simple.repl;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.interceptors.locking.OptimisticLockingInterceptor;
import org.infinispan.interceptors.locking.PessimisticLockingInterceptor;
import org.infinispan.interceptors.totalorder.TotalOrderInterceptor;
import org.infinispan.interceptors.totalorder.TotalOrderVersionedEntryWrappingInterceptor;
import org.infinispan.interceptors.totalorder.TotalOrderVersionedReplicationInterceptor;
import org.testng.annotations.Test;

import static junit.framework.Assert.assertTrue;
import static org.testng.Assert.assertFalse;

/**
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", testName = "tx.totalorder.simple.repl.SyncPrepareWriteSkewTotalOrderTest")
public class SyncPrepareWriteSkewTotalOrderTest extends FullAsyncTotalOrderTest {

   public SyncPrepareWriteSkewTotalOrderTest() {
      super(3, CacheMode.REPL_SYNC, false, true, false);
   }

   @Override
   public void testSinglePhaseTotalOrder() {
      assertFalse(Configurations.isOnePhaseTotalOrderCommit(cache(0).getCacheConfiguration()));
   }

   @Override
   public void testInteceptorChain() {
      InterceptorChain ic = advancedCache(0).getComponentRegistry().getComponent(InterceptorChain.class);
      assertTrue(ic.containsInterceptorType(TotalOrderInterceptor.class));
      assertTrue(ic.containsInterceptorType(TotalOrderVersionedReplicationInterceptor.class));
      assertTrue(ic.containsInterceptorType(TotalOrderVersionedEntryWrappingInterceptor.class));
      assertFalse(ic.containsInterceptorType(OptimisticLockingInterceptor.class));
      assertFalse(ic.containsInterceptorType(PessimisticLockingInterceptor.class));
   }
}
