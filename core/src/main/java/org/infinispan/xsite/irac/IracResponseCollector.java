package org.infinispan.xsite.irac;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.status.DefaultTakeOfflineManager;

/**
 * A response collector for an asynchronous cross site requests.
 * <p>
 * Multiple keys are batched together in a single requests. The remote site sends a {@link IntSet} back where if bit
 * {@code n} is set, it means the {@code n}th key in the batch failed to be applied (example, lock failed to be
 * acquired), and it needs to be retried.
 * <p>
 * If an {@link Exception} is received (example, timed-out waiting for the remote site ack), it assumes all keys in the
 * batch aren't applied, and they are retried.
 * <p>
 * When the response (or exception) is received, {@link IracResponseCompleted#onResponseCompleted(IracXSiteBackup,
 * IracBatchSendResult, Collection)} is invoked with the global result in {@link IracBatchSendResult} and a collection
 * with all the successfully applied keys. Once the listener finishes execution, the {@link CompletableFuture} completes
 * (completed value not relevant, and it is never completed exceptionally).
 *
 * @author Pedro Ruivo
 * @since 12
 */
public class IracResponseCollector extends CompletableFuture<Void> implements BiConsumer<IntSet, Throwable> {

   private static final Log log = LogFactory.getLog(IracResponseCollector.class);
   private IracBatchSendResult result = IracBatchSendResult.OK;
   private final IracXSiteBackup backup;
   private final IntSet failedKeys;
   private final String cacheName;
   private final Collection<IracManagerKeyState> batch;
   private final IracResponseCompleted listener;

   public IracResponseCollector(String cacheName, IracXSiteBackup backup, Collection<IracManagerKeyState> batch, IracResponseCompleted listener) {
      this.cacheName = cacheName;
      this.backup = backup;
      this.batch = batch;
      this.listener = listener;
      failedKeys = IntSets.concurrentSet(batch.size());
   }

   @Override
   public void accept(IntSet rspIntSet, Throwable throwable) {
      boolean trace = log.isTraceEnabled();
      if (throwable != null) {
         if (DefaultTakeOfflineManager.isCommunicationError(throwable)) {
            //in case of communication error, we need to back-off.
            backup.enableBackOffMode();
            result = IracBatchSendResult.BACK_OFF_AND_RETRY;
         } else if (result == IracBatchSendResult.OK) {
            //don't overwrite communication errors
            backup.disableBackOffMode();
            result = IracBatchSendResult.RETRY;
         }
         if (backup.logExceptions()) {
            log.warnXsiteBackupFailed(cacheName, backup.getSiteName(), throwable);
         } else if (trace) {
            log.tracef(throwable, "[IRAC] Encountered issues while backing up data for cache %s to site %s", cacheName, backup.getSiteName());
         }
         allKeysFailed();
      } else {
         if (trace) {
            log.tracef("[IRAC] Received response from site %s: %s", backup.getSiteName(), rspIntSet);
         }
         backup.disableBackOffMode();
         mergeIntSetResult(rspIntSet);
         // if some keys failed to apply, we need to retry.
         if (rspIntSet != null && !rspIntSet.isEmpty() && result == IracBatchSendResult.OK) {
            result = IracBatchSendResult.RETRY;
         }
      }

      Collection<IracManagerKeyState> successfulSent = new ArrayList<>(batch.size());
      int index = 0;
      for (IracManagerKeyState state : batch) {
         if (hasKeyFailed(index)) {
            state.retry();
         } else {
            successfulSent.add(state);
            state.successFor(backup);
         }
      }
      listener.onResponseCompleted(backup, result, successfulSent);
      this.complete(null);
   }

   private void allKeysFailed() {
      for (int i = 0; i < batch.size(); i++) {
         failedKeys.set(i);
      }
   }

   private void mergeIntSetResult(IntSet rsp) {
      if (rsp != null) failedKeys.addAll(rsp);
   }

   private boolean hasKeyFailed(int index) {
      return failedKeys.contains(index);
   }

   @FunctionalInterface
   public interface IracResponseCompleted {
      void onResponseCompleted(IracXSiteBackup site, IracBatchSendResult result, Collection<IracManagerKeyState> successfulSent);
   }
}
