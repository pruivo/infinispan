package org.infinispan.tx.gmu;

import org.infinispan.commands.tx.GMUPrepareCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.CallInterceptor;
import org.infinispan.interceptors.base.BaseCustomInterceptor;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Set;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", testName = "tx.gmu.DistSimpleTest")
public class DistSimpleTest extends SimpleTest {

   public void testReadSet() throws Exception {
      try {
         Object read = newKey(1);
         Object write = newKey(0);
         AffectedKeysInterceptor interceptor = new AffectedKeysInterceptor();
         cache(0).getAdvancedCache().addInterceptorBefore(interceptor, CallInterceptor.class);

         tm(0).begin();
         cache(0).get(read);
         cache(0).put(write, VALUE_1);
         tm(0).commit();
         Assert.assertNotNull(interceptor.affectedKeys);
         Assert.assertTrue(interceptor.affectedKeys.containsAll(Arrays.asList(read, write)));
      } finally {
         cache(0).getAdvancedCache().removeInterceptor(AffectedKeysInterceptor.class);
      }
   }

   public void testReadSet2() throws Exception {
      try {
         Object read = newKey(0);
         Object write = newKey(1);
         AffectedKeysInterceptor interceptor = new AffectedKeysInterceptor();
         cache(0).getAdvancedCache().addInterceptorBefore(interceptor, CallInterceptor.class);

         tm(0).begin();
         cache(0).get(read);
         cache(0).put(write, VALUE_1);
         tm(0).commit();
         Assert.assertNotNull(interceptor.affectedKeys);
         Assert.assertTrue(interceptor.affectedKeys.containsAll(Arrays.asList(read, write)));
      } finally {
         cache(0).getAdvancedCache().removeInterceptor(AffectedKeysInterceptor.class);
      }
   }

   @Override
   protected CacheMode cacheMode() {
      return CacheMode.DIST_SYNC;
   }

   @Override
   protected void decorate(ConfigurationBuilder builder) {
      builder.clustering().hash().numOwners(1);
   }

   public class AffectedKeysInterceptor extends BaseCustomInterceptor {

      private volatile Set<Object> affectedKeys = null;

      @Override
      public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
         Assert.assertTrue(command instanceof GMUPrepareCommand);
         if (ctx.isOriginLocal()) {
            affectedKeys = ctx.getAffectedKeys();
         }
         return invokeNextInterceptor(ctx, command);
      }
   }
}
