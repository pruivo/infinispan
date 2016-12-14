package org.infinispan.remoting;

import static org.infinispan.test.AbstractCacheTest.getDefaultClusteredCacheConfig;
import static org.infinispan.test.TestingUtil.extractComponent;
import static org.infinispan.test.TestingUtil.extractGlobalComponentRegistry;
import static org.infinispan.test.fwk.TestCacheManagerFactory.createClusteredCacheManager;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotSame;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.Cache;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commons.executors.BlockingThreadPoolExecutorFactory;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.inboundhandler.PerCacheInboundInvocationHandler;
import org.infinispan.remoting.inboundhandler.Reply;
import org.infinispan.statetransfer.StateTransferLock;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.util.concurrent.BlockingTaskAwareExecutorService;
import org.infinispan.util.concurrent.CompletableFutures;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Tests if the pool is correctly configured and behaves as expected.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
@Test(groups = "functional", testName = "remoting.BlockingTaskAwareMergePoolTest")
public class BlockingTaskAwareMergePoolTest extends AbstractInfinispanTest {

   private static final Reply DUMMY_REPLY = returnValue -> {

   };
   private EmbeddedCacheManager cacheManager;

   private static GlobalConfigurationBuilder createGlobalConfigurationBuilder(boolean mergePools) {
      GlobalConfigurationBuilder builder = GlobalConfigurationBuilder.defaultClusteredBuilder();
      if (!mergePools) {
         builder.transport().remoteCommandThreadPool()
               .threadPoolFactory(BlockingThreadPoolExecutorFactory.create(1, 0));
      }
      return builder;
   }

   private static ClusteredGetCommand mockCommand(CompletableFuture<Thread> completableFuture) throws Throwable {
      ClusteredGetCommand mock = mock(ClusteredGetCommand.class);
      when(mock.canBlock()).thenReturn(true);
      when(mock.invokeAsync()).then(invocation -> {
         completableFuture.complete(Thread.currentThread());
         return CompletableFutures.completedNull();
      });
      return mock;
   }

   @AfterMethod(alwaysRun = true)
   public void tearDown() {
      if (cacheManager != null) {
         cacheManager.stop();
         cacheManager = null;
      }
   }

   public void testWithPoolsMerged() throws Throwable {
      createCacheManager(true);
      final Cache cache = cacheManager.getCache();
      final int topologyId = extractComponent(cache, StateTransferManager.class).getCacheTopology().getTopologyId();
      PerCacheInboundInvocationHandler handler = extractComponent(cache, PerCacheInboundInvocationHandler.class);
      final BlockingTaskAwareExecutorService executorService = extractGlobalComponentRegistry(cacheManager)
            .getComponent(BlockingTaskAwareExecutorService.class,
                  KnownComponentNames.REMOTE_COMMAND_EXECUTOR);
      final StateTransferLock stateTransferLock = extractComponent(cache, StateTransferLock.class);
      CompletableFuture<Thread> executionThread = new CompletableFuture<>();
      ClusteredGetCommand replicableCommand = mockCommand(executionThread);
      when(replicableCommand.getTopologyId()).thenReturn(topologyId);
      handler.handle(replicableCommand, DUMMY_REPLY, DeliverOrder.NONE);
      assertEquals(Thread.currentThread(), executionThread.get(10, TimeUnit.SECONDS));

      executionThread = new CompletableFuture<>();
      replicableCommand = mockCommand(executionThread);
      when(replicableCommand.getTopologyId()).thenReturn(topologyId + 1);
      handler.handle(replicableCommand, DUMMY_REPLY, DeliverOrder.NONE);
      try {
         executionThread.get(10, TimeUnit.SECONDS);
         fail();
      } catch (TimeoutException e) {
         log.debug("Expected exception", e);
      }
      //cheats!
      stateTransferLock.notifyTopologyInstalled(topologyId + 1);
      stateTransferLock.notifyTransactionDataReceived(topologyId + 1);
      executorService.checkForReadyTasks();
      assertNotSame(Thread.currentThread(), executionThread.get(10, TimeUnit.SECONDS));
      //check if it is a jgroups thread!
      assertTrue(executionThread.get().getName().startsWith("jgroups-"));
   }

   public void testWithoutPoolsMerged() throws Throwable {
      createCacheManager(false);
      final Cache cache = cacheManager.getCache();
      final int topologyId = extractComponent(cache, StateTransferManager.class).getCacheTopology().getTopologyId();
      PerCacheInboundInvocationHandler handler = extractComponent(cache, PerCacheInboundInvocationHandler.class);
      CompletableFuture<Thread> executionThread = new CompletableFuture<>();
      ClusteredGetCommand replicableCommand = mockCommand(executionThread);
      when(replicableCommand.getTopologyId()).thenReturn(topologyId);
      handler.handle(replicableCommand, DUMMY_REPLY, DeliverOrder.NONE);
      assertNotSame(Thread.currentThread(), executionThread.get(10, TimeUnit.SECONDS));
      assertFalse(executionThread.get().getName().startsWith("jgroups-"));
   }

   private void createCacheManager(boolean mergePools) {
      if (cacheManager != null) {
         throw new IllegalStateException();
      }
      cacheManager = createClusteredCacheManager(createGlobalConfigurationBuilder(mergePools),
            getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
   }
}
