package org.infinispan.container.versioning.irac;

import static org.infinispan.commons.util.concurrent.CompletableFutures.completedNull;
import static org.infinispan.context.Flag.CACHE_MODE_LOCAL;
import static org.infinispan.context.Flag.FAIL_SILENTLY;
import static org.infinispan.context.Flag.IGNORE_RETURN_VALUES;
import static org.infinispan.context.Flag.STREAM_TOMBSTONES;
import static org.infinispan.context.Flag.ZERO_LOCK_ACQUISITION_TIMEOUT;
import static org.infinispan.reactive.publisher.impl.DeliveryGuarantee.AT_MOST_ONCE;

import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.infinispan.cache.impl.InvocationHelper;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.irac.IracTombstoneRemoteSiteCheckCommand;
import org.infinispan.commands.write.RemoveTombstoneCommand;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.XSiteStateTransferConfiguration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.reactive.RxJavaInterop;
import org.infinispan.reactive.publisher.impl.LocalPublisherManager;
import org.infinispan.reactive.publisher.impl.SegmentPublisherSupplier;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.util.ExponentialBackOff;
import org.infinispan.util.concurrent.AggregateCompletionStage;
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.XSiteBackup;
import org.infinispan.xsite.irac.DefaultIracManager;
import org.infinispan.xsite.irac.IracExecutor;
import org.infinispan.xsite.irac.IracManager;
import org.infinispan.xsite.irac.IracXSiteBackup;
import org.infinispan.xsite.status.SiteState;
import org.infinispan.xsite.status.TakeOfflineManager;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.flowables.GroupedFlowable;
import io.reactivex.rxjava3.functions.Predicate;
import net.jcip.annotations.GuardedBy;

/**
 * A default implementation for {@link IracTombstoneManager}.
 * <p>
 * This class is responsible to keep track of the tombstones for the IRAC algorithm. Tombstones are used when a key is
 * removed but its metadata is necessary to detect possible conflicts in this and remote sites. When all sites have
 * updated the key, the tombstone can be removed.
 * <p>
 * Tombstones are removed periodically in the background.
 *
 * @since 14.0
 */
@Scope(Scopes.NAMED_CACHE)
public class DefaultIracTombstoneManager implements IracTombstoneManager {

   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   private static final BiConsumer<Void, Throwable> TRACE_ROUND_COMPLETED = (__, throwable) -> {
      if (throwable != null) {
         log.trace("[IRAC] Tombstone cleanup round failed!", throwable);
      } else {
         log.trace("[IRAC] Tombstone cleanup round finished!");
      }
   };
   private static final Predicate<SegmentPublisherSupplier.Notification<CacheEntry<Object, Object>>> IS_TOMBSTONE = n -> n.isValue() && n.value().isTombstone();
   private static final long RM_TOMBSTONE_FLAGS = EnumUtil.bitSetOf(FAIL_SILENTLY, ZERO_LOCK_ACQUISITION_TIMEOUT, IGNORE_RETURN_VALUES);
   private static final long STREAM_FLAGS = EnumUtil.bitSetOf(STREAM_TOMBSTONES, CACHE_MODE_LOCAL);

   @Inject DistributionManager distributionManager;
   @Inject RpcManager rpcManager;
   @Inject CommandsFactory commandsFactory;
   @Inject TakeOfflineManager takeOfflineManager;
   @Inject ComponentRef<IracManager> iracManager;
   @ComponentName(KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR)
   @Inject ScheduledExecutorService scheduledExecutorService;
   @Inject BlockingManager blockingManager;
   @Inject InternalDataContainer<?, ?> dataContainer;
   @Inject LocalPublisherManager<Object, Object> localPublisherManager;
   @Inject ComponentRef<InvocationHelper> invocationHelper;
   private final IracExecutor iracExecutor;
   private final Collection<IracXSiteBackup> asyncBackups;
   private final Scheduler scheduler;
   private volatile boolean stopped = true;
   private final int batchSize;
   private final int segmentCount;

   public DefaultIracTombstoneManager(Configuration configuration) {
      iracExecutor = new IracExecutor(this::performCleanup);
      asyncBackups = DefaultIracManager.asyncBackups(configuration);
      scheduler = new Scheduler(configuration.sites().tombstoneMapSize(), configuration.sites().maxTombstoneCleanupDelay());
      batchSize = configuration.sites().asyncBackupsStream()
            .map(BackupConfiguration::stateTransfer)
            .map(XSiteStateTransferConfiguration::chunkSize)
            .reduce(1, Integer::max);
      segmentCount = configuration.clustering().hash().numSegments();

   }

   @Start
   public void start() {
      Transport transport = rpcManager.getTransport();
      transport.checkCrossSiteAvailable();
      String localSiteName = transport.localSiteName();
      asyncBackups.removeIf(xSiteBackup -> localSiteName.equals(xSiteBackup.getSiteName()));
      iracExecutor.setBackOff(ExponentialBackOff.NO_OP);
      iracExecutor.setExecutor(blockingManager.asExecutor(commandsFactory.getCacheName() + "-tombstone-cleanup"));
      stopped = false;
      scheduler.disabled = false;
      scheduler.scheduleWithCurrentDelay();
   }

   @Stop
   public void stop() {
      stopped = true;
      stopCleanupTask();
   }

   // for testing purposes only!
   public void stopCleanupTask() {
      scheduler.disable();
   }

   @Override
   public int size() {
      return (int) dataContainer.numberOfTombstones();
   }

   @Override
   public boolean isTaskRunning() {
      return scheduler.running;
   }

   @Override
   public long getCurrentDelayMillis() {
      return scheduler.currentDelayMillis;
   }

   // Testing purposes
   public void startCleanupTombstone() {
      iracExecutor.run();
   }

   // Testing purposes
   public void runCleanupAndWait() {
      performCleanup().toCompletableFuture().join();
   }

   // Testing purposes
   public boolean contains(IracTombstoneInfo tombstone) {
      InternalCacheEntry<?, ?> entry = dataContainer.peek(tombstone.getSegment(), tombstone.getKey());
      return entry != null && entry.isTombstone() && tombstone.getMetadata().equals(entry.getInternalMetadata().iracMetadata());
   }

   private CompletionStage<Void> performCleanup() {
      if (stopped) {
         return completedNull();
      }
      boolean trace = log.isTraceEnabled();

      if (trace) {
         log.trace("[IRAC] Starting tombstone cleanup round.");
      }

      scheduler.onTaskStarted(size());

      CompletionStage<Void> stage = iterate()
            .groupBy(SegmentPublisherSupplier.Notification::valueSegment)
            // We are using flatMap to allow for actions to be done in parallel, but the requests in each action
            // must be performed sequentially
            .flatMap(this::checkRemoteSite, true, segmentCount, segmentCount)
            .lastStage(null);
      if (trace) {
         stage = stage.whenComplete(TRACE_ROUND_COMPLETED);
      }
      return stage.whenComplete(scheduler);
   }

   private Flowable<SegmentPublisherSupplier.Notification<CacheEntry<Object, Object>>> iterate() {
      IntSet segments = distributionManager.getCacheTopology().getLocalPrimarySegments();
      return Flowable.fromPublisher(localPublisherManager.entryPublisher(segments, null, null, STREAM_FLAGS, AT_MOST_ONCE, Function.identity())
                  .publisherWithSegments())
            .filter(IS_TOMBSTONE);
   }

   private DistributionInfo getSegmentDistribution(int segment) {
      return distributionManager.getCacheTopology().getSegmentDistribution(segment);
   }

   private Flowable<Void> checkRemoteSite(GroupedFlowable<Integer, ? extends SegmentPublisherSupplier.Notification<CacheEntry<Object, Object>>> flowable) {
      int segment = flowable.getKey();
      if (!getSegmentDistribution(segment).isPrimary()) {
         // topology changed?
         return Flowable.empty();
      }
      return flowable.map(SegmentPublisherSupplier.Notification::value)
            // if update is pending in local IracManager, no need to check
            .filter(e -> !iracManager.running().containsKey(e.getKey()))
            .buffer(batchSize)
            .concatMapDelayError(tombstoneMap -> new CleanupTask(segment, tombstoneMap).check());
   }

   private final class CleanupTask implements Function<Void, CompletionStage<Void>> {
      private final Collection<? extends CacheEntry<Object, Object>> tombstoneToCheck;
      private final IntSet tombstoneToKeep;
      private final int id;
      private final int segment;
      private volatile boolean failedToCheck;

      private CleanupTask(int segment, Collection<? extends CacheEntry<Object, Object>> tombstoneToCheck) {
         this.segment = segment;
         this.tombstoneToCheck = tombstoneToCheck;
         tombstoneToKeep = IntSets.concurrentSet(tombstoneToCheck.size());
         failedToCheck = false;
         id = tombstoneToCheck.hashCode();
      }

      Flowable<Void> check() {
         if (log.isTraceEnabled()) {
            log.tracef("[cleanup-task-%d] Running cleanup task on segment %d with %s tombstones to check", id, segment, tombstoneToCheck.size());
         }
         if (tombstoneToCheck.isEmpty()) {
            if (log.isTraceEnabled()) {
               log.tracef("[cleanup-task-%d] Nothing to remove", id);
            }
            return Flowable.empty();
         }
         List<Object> keys = tombstoneToCheck.stream()
               .map(CacheEntry::getKey)
               .collect(Collectors.toList());
         IracTombstoneRemoteSiteCheckCommand cmd = commandsFactory.buildIracTombstoneRemoteSiteCheckCommand(keys);
         // if one of the site return true (i.e. the key is in updateKeys map, then do not remove it)
         AggregateCompletionStage<Void> stage = CompletionStages.aggregateCompletionStage();
         for (XSiteBackup backup : asyncBackups) {
            if (takeOfflineManager.getSiteState(backup.getSiteName()) == SiteState.OFFLINE) {
               continue; // backup is offline
            }
            // we don't need the tombstone to query the remote site
            stage.dependsOn(rpcManager.invokeXSite(backup, cmd).thenAccept(this::mergeIntSet));
         }
         // in case of exception, keep the tombstone
         return RxJavaInterop.voidCompletionStageToFlowable(blockingManager.thenComposeBlocking(
               stage.freeze().exceptionally(this::onException), this, "tombstone-response"));
      }

      private void mergeIntSet(IntSet rsp) {
         if (log.isTraceEnabled()) {
            log.tracef("[cleanup-task-%d] Received response: %s", id, rsp);
         }
         tombstoneToKeep.addAll(rsp);
      }

      private Void onException(Throwable ignored) {
         if (log.isTraceEnabled()) {
            log.tracef(ignored, "[cleanup-task-%d] Received exception", id);
         }
         failedToCheck = true;
         return null;
      }

      @Override
      public CompletionStage<Void> apply(Void aVoid) {
         RemoveTombstoneCommand cmd = commandsFactory.buildRemoveTombstoneCommand(segment, RM_TOMBSTONE_FLAGS, tombstoneToCheck.size());
         forEachTombstoneToRemove(e -> cmd.setInternalMetadata(e.getKey(), e.getInternalMetadata()));

         if (log.isTraceEnabled()) {
            log.tracef("[cleanup-task-%d] Removing %d tombstones.", id, cmd.getTombstones().size());
         }

         if (cmd.isEmpty()) {
            // nothing to remove
            return completedNull();
         }
         InvocationHelper helper = invocationHelper.running();
         return helper.invokeAsync(helper.createNonTxInvocationContext(), cmd);
      }

      void forEachTombstoneToRemove(Consumer<? super CacheEntry<Object, Object>> consumer) {
         if (failedToCheck) {
            return;
         }
         int index = 0;
         for (CacheEntry<Object, Object> tombstone : tombstoneToCheck) {
            if (tombstoneToKeep.contains(index++)) {
               continue;
            }
            consumer.accept(tombstone);
         }
      }
   }

   private final class Scheduler implements BiConsumer<Void, Throwable> {
      final int targetSize;
      final long maxDelayMillis;

      int preCleanupSize;
      int previousPostCleanupSize;

      long currentDelayMillis;
      volatile boolean running;
      volatile boolean disabled;
      @GuardedBy("this")
      ScheduledFuture<?> future;

      private Scheduler(int targetSize, long maxDelayMillis) {
         this.targetSize = targetSize;
         this.maxDelayMillis = maxDelayMillis;
         currentDelayMillis = maxDelayMillis / 2;
      }

      void onTaskStarted(int size) {
         running = true;
         preCleanupSize = size;
      }

      void onTaskCompleted(int postCleanupSize) {
         if (postCleanupSize >= targetSize) {
            // The tombstones map is already at or above the target size, start a new cleanup round immediately
            // Keep the delay >= 1 to simplify the tombstoneCreationRate calculation
            currentDelayMillis = 1;
         } else {
            // Estimate how long it would take for the tombstones map to reach the target size
            double tombstoneCreationRate = (preCleanupSize - previousPostCleanupSize) * 1.0 / currentDelayMillis;
            double estimationMillis;
            if (tombstoneCreationRate <= 0) {
               // The tombstone map will never reach the target size, use the maximum delay
               estimationMillis = maxDelayMillis;
            } else {
               // Ensure that 1 <= estimation <= maxDelayMillis
               estimationMillis = Math.min((targetSize - postCleanupSize) / tombstoneCreationRate + 1, maxDelayMillis);
            }
            // Use a geometric average between the current estimation and the previous one
            // to dampen the changes as the rate changes from one interval to the next
            // (especially when the interval duration is very short)
            currentDelayMillis = Math.round(Math.sqrt(currentDelayMillis * estimationMillis));
         }
         previousPostCleanupSize = postCleanupSize;
         scheduleWithCurrentDelay();
      }

      synchronized void scheduleWithCurrentDelay() {
         running = false;
         if (stopped || disabled) {
            return;
         }
         if (future != null) {
            future.cancel(true);
         }
         future = scheduledExecutorService.schedule(iracExecutor, currentDelayMillis, TimeUnit.MILLISECONDS);
      }

      synchronized void disable() {
         disabled = true;
         if (future != null) {
            future.cancel(true);
            future = null;
         }
      }

      @Override
      public void accept(Void unused, Throwable throwable) {
         // invoked after the cleanup round
         onTaskCompleted(size());
      }
   }
}
