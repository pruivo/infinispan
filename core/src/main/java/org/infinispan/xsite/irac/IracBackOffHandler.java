package org.infinispan.xsite.irac;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.util.ExponentialBackOff;

/**
 * A global back-off handler for IRAC.
 * </p>
 * Manages the back-off time of each site independently. Each site can be put/removed from back-off without affecting
 * the back-off behavior of other sites.
 * </p>
 * Once a site is put to back-off mode, a {@link CompletableFuture} is scheduled asynchronously, using the defined
 * {@link Executor} to deactivate the back-off mode. Once it is disabled, it only applies to the specified site.
 *
 * @since 15.0
 */
public class IracBackOffHandler {

   private final Executor executor;
   private final Map<String, CompletableFuture<Void>> runnables;

   public IracBackOffHandler(Collection<IracXSiteBackup> backups, Supplier<ExponentialBackOff> createBackOff, Executor executor) {
      this.executor = executor;
      this.runnables = new ConcurrentHashMap<>();
      for (IracXSiteBackup backup : backups) {
         backup.useBackOff(createBackOff.get());
         runnables.put(backup.getSiteName(), CompletableFutures.completedNull());
      }
   }

   public void enableBackOff(IracXSiteBackup site, Runnable r) {
      runnables.compute(site.getSiteName(), (key, runnable) -> {
         assert runnable != null : "Unknown site " + key;
         return runnable.thenCompose(ignore -> site.asyncBackOff())
               .thenRunAsync(site, executor)
               .whenComplete((ignore, t) -> r.run());
      });
   }

   public void resetBackOff(IracXSiteBackup site) {
      runnables.compute(site.getSiteName(), (key, runnable) -> {
         assert runnable != null : "Unknown site " + key;
         site.resetBackOff();
         return CompletableFutures.completedNull();
      });
   }
}
