package org.infinispan.remoting;

import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.TxInterceptor;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.transaction.NotSupportedException;
import javax.transaction.SystemException;

/**
 * Test timeout exception
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", testName = "remoting.TimeoutTest")
public class TimeoutTest extends MultipleCacheManagersTest {

   private static final String CACHE_NAME = "_timeout_cluster_";

   @BeforeMethod
   public void setUp() {
      long timeout = cache(0, CACHE_NAME).getCacheConfiguration().clustering().sync().replTimeout();
      cache(0, CACHE_NAME).getAdvancedCache().addInterceptorAfter(new BlockPrepareInterceptor(timeout), TxInterceptor.class);
   }

   public void testTimeout() throws SystemException, NotSupportedException {
      tm(1, CACHE_NAME).begin();
      cache(1, CACHE_NAME).put("key", "value");
      try {
         tm(1, CACHE_NAME).commit();
         assert false;
      } catch (Exception e) {

      }
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder configurationBuilder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      configurationBuilder.locking().isolationLevel(IsolationLevel.REPEATABLE_READ).writeSkewCheck(true);
      configurationBuilder.versioning().enabled(true).scheme(VersioningScheme.SIMPLE);
      createClusteredCaches(4, CACHE_NAME, configurationBuilder);
   }

   private class BlockPrepareInterceptor extends CommandInterceptor {

      private long timeout;

      public BlockPrepareInterceptor(long timeout) {
         this.timeout = timeout;
      }

      @Override
      public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
         if (!ctx.isOriginLocal()) {
            synchronized (this) {
               this.wait(timeout * 2);
            }
         }
         return invokeNextInterceptor(ctx, command);
      }
   }
}
