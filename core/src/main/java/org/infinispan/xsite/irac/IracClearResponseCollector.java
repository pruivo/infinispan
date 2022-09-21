package org.infinispan.xsite.irac;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.infinispan.commands.write.ClearCommand;
import org.infinispan.util.concurrent.CountDownRunnable;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.status.DefaultTakeOfflineManager;

/**
 * Used by asynchronous cross-site replication, it aggregates response from multiple sites and returns {@link
 * IracBatchSendResult}.
 * <p>
 * This collector assumes the request is a {@link ClearCommand}, and never completes exceptionally.
 *
 * @author Pedro Ruivo
 * @since 14.0
 */
public class IracClearResponseCollector implements Runnable {

   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   private static final AtomicReferenceFieldUpdater<IracClearResponseCollector, IracBatchSendResult> RESULT_UPDATED = AtomicReferenceFieldUpdater.newUpdater(IracClearResponseCollector.class, IracBatchSendResult.class, "result");

   private volatile IracBatchSendResult result = IracBatchSendResult.OK;
   private final String cacheName;
   private final CountDownRunnable countDownRunnable;
   private final CompletableFuture<Void> complete = new CompletableFuture<>();
   private final Collection<IracXSiteBackup> failedBackups = Collections.synchronizedList(new ArrayList<>());
   private final IracClearResponseHandler handler;

   public IracClearResponseCollector(String cacheName, IracClearResponseHandler handler) {
      this.cacheName = cacheName;
      countDownRunnable = new CountDownRunnable(this);
      this.handler = handler;
   }

   public void dependsOn(IracXSiteBackup backup, CompletionStage<Void> request) {
      countDownRunnable.increment();
      request.whenComplete((bitSet, throwable) -> onResponse(backup, throwable));
   }

   public CompletionStage<Void> freeze() {
      countDownRunnable.freeze();
      return complete;
   }

   private void onResponse(IracXSiteBackup backup, Throwable throwable) {
      try {
         boolean trace = log.isTraceEnabled();
         if (throwable != null) {
            if (DefaultTakeOfflineManager.isCommunicationError(throwable)) {
               //in case of communication error, we need to back-off.
               RESULT_UPDATED.set(this, IracBatchSendResult.BACK_OFF_AND_RETRY);
               backup.enableBackOffMode();
               failedBackups.add(backup);
            } else {
               //don't overwrite communication errors
               if (RESULT_UPDATED.compareAndSet(this, IracBatchSendResult.OK, IracBatchSendResult.RETRY)) {
                  backup.disableBackOffMode();
               }
            }
            if (backup.logExceptions()) {
               log.warnXsiteBackupFailed(cacheName, backup.getSiteName(), throwable);
            } else if (trace) {
               log.tracef(throwable, "Encountered issues while backing clear command for cache %s to site %s", cacheName, backup.getSiteName());
            }
         } else if (trace) {
            log.tracef("Received clear response from %s (%d remaining)", backup.getSiteName(), countDownRunnable.missing());
         }

      } finally {
         countDownRunnable.decrement();
      }
   }

   @Override
   public void run() {
      handler.onResponse(result, failedBackups);
      // executed after all results are received (or timed out)!
      complete.complete(null);
   }

   @FunctionalInterface
   public interface IracClearResponseHandler {

      void onResponse(IracBatchSendResult result, Collection<IracXSiteBackup> failedBackups);
   }
}
