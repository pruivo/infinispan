package org.infinispan.partitionhandling;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.transaction.TransactionManager;

import org.infinispan.AdvancedCache;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.conflict.MergePolicy;
import org.infinispan.distribution.MagicKey;
import org.infinispan.remoting.inboundhandler.AbstractDelegatingHandler;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.inboundhandler.PerCacheInboundInvocationHandler;
import org.infinispan.remoting.inboundhandler.Reply;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

/**
 * TODO! document this
 *
 * @author Pedro Ruivo
 * @since 12
 */
@Test(groups = "functional", testName = "partitionhandling.PartitionPessimisticTx")
public class PartitionPessimisticTx extends MultipleCacheManagersTest {

   private static final Log log = LogFactory.getLog(PartitionPessimisticTx.class);

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      builder.transaction().lockingMode(LockingMode.PESSIMISTIC).useSynchronization(true);
      builder.clustering().partitionHandling().mergePolicy(MergePolicy.NONE).whenSplit(PartitionHandling.DENY_READ_WRITES);
      builder.clustering().remoteTimeout(Integer.MAX_VALUE); // we don't want timeouts
      createClusteredCaches(5, TestDataSCI.INSTANCE, builder, new TransportFlags().withFD(true).withMerge(true));
   }

   public void testCache() throws Exception {
      final AdvancedCache<MagicKey, String> cache0 = this.<MagicKey, String>cache(0).getAdvancedCache();
      final TransactionManager tm = cache0.getTransactionManager();
      final ControlledInboundHandler handler1 = TestingUtil.wrapInboundInvocationHandler(cache(1), ControlledInboundHandler::new);
      final MagicKey key = new MagicKey(cache0, cache(1));

      Future<Void> f = fork(() -> {
         tm.begin();
         cache0.lock(key);
         AssertJUnit.assertNull(cache0.get(key));
         cache0.put(key, key.toString());
         tm.commit();
      });

      AssertJUnit.assertTrue(handler1.receivedLatch.await(30, TimeUnit.SECONDS));

      TestingUtil.getDiscardForCache(manager(1)).setDiscardAll(true);
      //TestingUtil.getDiscardForCache(manager(2)).setDiscardAll(true);

      //killMember(1, null, false);
      //killMember(1, null, true);

      f.get();

      eventuallyEquals(0, () -> TestingUtil.extractLockManager(cache0).getNumberOfLocksHeld());
   }

   private static class ControlledInboundHandler extends AbstractDelegatingHandler {

      private final CountDownLatch receivedLatch;

      private ControlledInboundHandler(PerCacheInboundInvocationHandler delegate) {
         super(delegate);
         receivedLatch = new CountDownLatch(1);
      }

      @Override
      public void handle(CacheRpcCommand command, Reply reply, DeliverOrder order) {
         if (command instanceof PrepareCommand) {
            log.debugf("Ignoring command %s", command);
            receivedLatch.countDown();
         } else {
            delegate.handle(command, reply, order);
         }
      }
   }
}
