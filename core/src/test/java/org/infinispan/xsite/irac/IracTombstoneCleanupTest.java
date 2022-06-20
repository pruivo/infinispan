package org.infinispan.xsite.irac;

import static org.infinispan.test.TestingUtil.extractCacheTopology;
import static org.infinispan.test.TestingUtil.extractComponent;
import static org.infinispan.test.TestingUtil.internalDataContainer;
import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.wrapComponent;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.infinispan.Cache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.triangle.RemoveTombstoneBackupWriteCommand;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.versioning.irac.DefaultIracTombstoneManager;
import org.infinispan.container.versioning.irac.IracEntryVersion;
import org.infinispan.container.versioning.irac.IracTombstoneManager;
import org.infinispan.container.versioning.irac.TopologyIracVersion;
import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ResponseCollector;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.util.AbstractDelegatingRpcManager;
import org.infinispan.util.ByteString;
import org.infinispan.xsite.status.TakeOfflineManager;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import net.jcip.annotations.GuardedBy;

/**
 * Basic tests for IRAC tombstone cleanup
 *
 * @since 14.0
 */
@Test(groups = "xsite", testName = "xsite.irac.IracTombstoneCleanupTest")
public class IracTombstoneCleanupTest extends MultipleCacheManagersTest {

   private static final String CACHE_NAME = "xsite-tombstone";
   private static final String SITE_NAME = "LON-1";

   @Override
   protected void createCacheManagers() throws Throwable {
      TransportFlags flags = new TransportFlags().withSiteIndex(0).withSiteName(SITE_NAME).withFD(true);
      createClusteredCaches(3, CACHE_NAME, cacheConfiguration(), flags);
      for (Cache<?, ?> cache : caches(CACHE_NAME)) {
         // stop automatic cleanup to avoid adding random events to the tests
         tombstoneManager(cache).stopCleanupTask();
         extractComponent(cache, TakeOfflineManager.class).takeSiteOffline("NYC");
      }
   }

   @AfterMethod(alwaysRun = true)
   @Override
   protected void clearContent() throws Throwable {
      for (Cache<String, String> cache : this.<String, String>caches(CACHE_NAME)) {
         recordingRpcManager(cache).stopRecording();
      }
      super.clearContent();
   }

   private static ConfigurationBuilder cacheConfiguration() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC);
      builder.clustering().hash().numOwners(2).numSegments(16);
      builder.sites().addBackup().site("NYC").strategy(BackupConfiguration.BackupStrategy.ASYNC).stateTransfer().chunkSize(1);
      return builder;
   }

   public void testPrimaryOwnerRoundCleanupsBackup(Method method) {
      String key = k(method);
      int segment = getSegment(key);
      Cache<String, String> pCache = findPrimaryOwner(segment);
      Cache<String, String> bCache = findBackupOwner(segment);

      PrivateMetadata metadata = dummyMetadata(1);

      internalDataContainer(pCache).putTombstone(segment, key, metadata);
      internalDataContainer(bCache).putTombstone(segment, key, metadata);

      assertEquals(1, tombstoneManager(pCache).size());
      assertEquals(1, tombstoneManager(bCache).size());

      RecordingRpcManager pRpcManager = recordingRpcManager(pCache);

      pRpcManager.startRecording();

      tombstoneManager(pCache).runCleanupAndWait();

      eventuallyEquals(0, () -> tombstoneManager(pCache).size());
      eventuallyEquals(0, () -> tombstoneManager(bCache).size());

      RemoveTombstoneBackupWriteCommand cmd = pRpcManager.findRemoveTombstoneCommand();

      assertNotNull(cmd);
      assertEquals(1, cmd.getTombstones().size());
      assertEquals(segment, cmd.getSegmentId());
      assertEquals(key, cmd.getTombstones().keySet().iterator().next());
      assertEquals(metadata, cmd.getTombstones().values().iterator().next());
   }

   public void testBackupOwnerRoundCleanupDoNotCleanupPrimary(Method method) {
      String key = k(method);
      int segment = getSegment(key);
      Cache<String, String> pCache = findPrimaryOwner(segment);
      Cache<String, String> bCache = findBackupOwner(segment);

      PrivateMetadata metadata = dummyMetadata(2);

      internalDataContainer(pCache).putTombstone(segment, key, metadata);
      internalDataContainer(bCache).putTombstone(segment, key, metadata);

      assertEquals(1, tombstoneManager(pCache).size());
      assertEquals(1, tombstoneManager(bCache).size());

      RecordingRpcManager pRpcManager = recordingRpcManager(pCache);
      RecordingRpcManager bRpcManager = recordingRpcManager(bCache);

      pRpcManager.startRecording();
      bRpcManager.startRecording();

      tombstoneManager(bCache).runCleanupAndWait();

      assertFalse(pRpcManager.isCommandSent());

      // check if nothing is removed... should we sleep here?
      assertEquals(1, tombstoneManager(pCache).size());
      assertEquals(1, tombstoneManager(bCache).size());

      // remove tombstone to avoid messing up with other tests
      internalDataContainer(pCache).remove(segment, key);
      internalDataContainer(bCache).remove(segment, key);
   }

   private Cache<String, String> findPrimaryOwner(int segment) {
      for (Cache<String, String> cache : this.<String, String>caches(CACHE_NAME)) {
         if (extractCacheTopology(cache).getSegmentDistribution(segment).isPrimary()) {
            return cache;
         }
      }
      throw new IllegalStateException("Find primary owner failed!");
   }

   private Cache<String, String> findBackupOwner(int segment) {
      for (Cache<String, String> cache : this.<String, String>caches(CACHE_NAME)) {
         if (extractCacheTopology(cache).getSegmentDistribution(segment).isWriteBackup()) {
            return cache;
         }
      }
      throw new IllegalStateException("Find backup owner failed!");
   }

   private static PrivateMetadata dummyMetadata(long version) {
      TopologyIracVersion iracVersion = TopologyIracVersion.create(1, version);
      IracMetadata iracMetadata = new IracMetadata(SITE_NAME, IracEntryVersion.newVersion(ByteString.fromString(SITE_NAME), iracVersion));
      return new PrivateMetadata.Builder().iracMetadata(iracMetadata).tombstone(true).build();
   }

   private int getSegment(String key) {
      return extractCacheTopology(cache(0, CACHE_NAME)).getSegment(key);
   }

   private static DefaultIracTombstoneManager tombstoneManager(Cache<?, ?> cache) {
      IracTombstoneManager tombstoneManager = extractComponent(cache, IracTombstoneManager.class);
      assert tombstoneManager instanceof DefaultIracTombstoneManager;
      return (DefaultIracTombstoneManager) tombstoneManager;
   }

   private static RecordingRpcManager recordingRpcManager(Cache<?, ?> cache) {
      RpcManager rpcManager = extractComponent(cache, RpcManager.class);
      if (rpcManager instanceof RecordingRpcManager) {
         return (RecordingRpcManager) rpcManager;
      }
      return wrapComponent(cache, RpcManager.class, RecordingRpcManager::new);
   }

   private static class RecordingRpcManager extends AbstractDelegatingRpcManager {

      @GuardedBy("this")
      private final List<CacheRpcCommand> commandList;
      private volatile boolean recording;

      RecordingRpcManager(RpcManager realOne) {
         super(realOne);
         commandList = new LinkedList<>();
      }

      RemoveTombstoneBackupWriteCommand findRemoveTombstoneCommand() {
         RemoveTombstoneBackupWriteCommand found = null;
         synchronized (this) {
            for (CacheRpcCommand rpcCommand : commandList) {
               if (rpcCommand.getClass() == RemoveTombstoneBackupWriteCommand.class) {
                  assertNull("More than one " + RemoveTombstoneBackupWriteCommand.class + " found in list: " + commandList, found);
                  found = (RemoveTombstoneBackupWriteCommand) rpcCommand;
               }
            }
         }
         return found;
      }

      boolean isCommandSent() {
         boolean found = false;
         synchronized (this) {
            for (CacheRpcCommand rpcCommand : commandList) {
               if (rpcCommand.getClass() == RemoveTombstoneBackupWriteCommand.class) {
                  assertFalse("More than one " + RemoveTombstoneBackupWriteCommand.class + " found in list: " + commandList, found);
                  found = true;
               }
            }
         }
         return found;
      }

      void startRecording() {
         synchronized (this) {
            commandList.clear();
         }
         recording = true;
      }

      void stopRecording() {
         recording = false;
         synchronized (this) {
            commandList.clear();
         }

      }

      @Override
      protected <T> CompletionStage<T> performRequest(Collection<Address> targets, ReplicableCommand command, ResponseCollector<T> collector, Function<ResponseCollector<T>, CompletionStage<T>> invoker, RpcOptions rpcOptions) {
         if (recording && command instanceof CacheRpcCommand) {
            synchronized (this) {
               commandList.add((CacheRpcCommand) command);
            }
         }
         return super.performRequest(targets, command, collector, invoker, rpcOptions);
      }

      @Override
      protected <T> void performSend(Collection<Address> targets, ReplicableCommand command, Function<ResponseCollector<T>, CompletionStage<T>> invoker) {
         if (recording && command instanceof CacheRpcCommand) {
            synchronized (this) {
               commandList.add((CacheRpcCommand) command);
            }
         }
         super.performSend(targets, command, invoker);
      }
   }
}
