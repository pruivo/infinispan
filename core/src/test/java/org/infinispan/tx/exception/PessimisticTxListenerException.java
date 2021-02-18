package org.infinispan.tx.exception;

import javax.transaction.TransactionManager;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.LockingMode;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

/**
 * TODO! document this
 *
 * @author Pedro Ruivo
 * @since 12
 */
@Test(groups = "functional", testName = "tx.exception.ListenerExceptionTest")
public class PessimisticTxListenerException extends MultipleCacheManagersTest {
   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      builder.transaction().autoCommit(false).lockingMode(LockingMode.PESSIMISTIC);
      createClusteredCaches(2, TestDataSCI.INSTANCE, builder);
   }

   public void testListener() throws Exception {
      cache(1).addListener(new MyListener());
      final MagicKey key0 = new MagicKey("key0", cache(0));
      final MagicKey key1 = new MagicKey("key1", cache(1));

      final TransactionManager transactionManager = cache(0).getAdvancedCache().getTransactionManager();
      transactionManager.begin();
      AssertJUnit.assertTrue(cache(0).getAdvancedCache().lock(key0));
      AssertJUnit.assertTrue(cache(0).getAdvancedCache().lock(key1));
      AssertJUnit.assertTrue(TestingUtil.extractLockManager(cache(0)).isLocked(key0));
      AssertJUnit.assertTrue(TestingUtil.extractLockManager(cache(1)).isLocked(key1));
      cache(0).put(key0, key0.toString());
      cache(0).put(key1, key1.toString());
      try {
         transactionManager.commit();
      } catch (Exception e) {
         e.printStackTrace();
      }

      AssertJUnit.assertFalse(TestingUtil.extractLockManager(cache(0)).isLocked(key0));
      AssertJUnit.assertFalse(TestingUtil.extractLockManager(cache(1)).isLocked(key1));
   }

   @Listener(primaryOnly = true, observation = Listener.Observation.POST)
   public static final class MyListener {

      @CacheEntryCreated
      public void onEntryCreated(CacheEntryCreatedEvent<MagicKey, String> event) {
         throw new IllegalStateException();
      }

   }
}
