package org.infinispan.util;

import static org.infinispan.metadata.impl.PrivateMetadata.getBuilder;

import java.util.Optional;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.container.versioning.irac.IracEntryVersion;
import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.util.logging.LogSupplier;

/**
 * Utility methods from IRAC (async cross-site replication)
 *
 * @author Pedro Ruivo
 * @since 12.0
 */
public final class IracUtils {

   private IracUtils() {
   }

   public static Optional<IracMetadata> findIracMetadataFromCacheEntry(CacheEntry<?, ?> entry) {
      PrivateMetadata privateMetadata = entry.getInternalMetadata();
      if (privateMetadata == null) {
         return Optional.empty();
      }
      return Optional.ofNullable(privateMetadata.iracMetadata());
   }

   public static IracEntryVersion getIracVersionFromCacheEntry(CacheEntry<?, ?> entry) {
      return findIracMetadataFromCacheEntry(entry).map(IracMetadata::getVersion).orElse(null);
   }

   /**
    * Stores the {@link IracMetadata} into {@link CacheEntry}.
    * <p>
    * If the entry is marked from removal, then the tombstone flag is set using
    * {@link MVCCEntry#setTombstone(boolean)}.
    *
    * @param entry       The {@link CacheEntry} to update.
    * @param metadata    The {@link IracMetadata} to store.
    * @param logSupplier The {@link LogSupplier} to log the {@link IracMetadata} and the key.
    */
   public static void setIracMetadata(CacheEntry<?, ?> entry, IracMetadata metadata, LogSupplier logSupplier) {
      assert metadata != null : "[IRAC] Metadata must not be null!";
      assert entry != null;
      assert entry instanceof MVCCEntry;
      Object key = entry.getKey();
      if (entry.isRemoved()) {
         logTombstoneAssociated(key, metadata, logSupplier);
         updateCacheEntryMetadata(entry, metadata, true);
         ((MVCCEntry<?, ?>) entry).setTombstone(true);
      } else {
         logIracMetadataAssociated(key, metadata, logSupplier);
         updateCacheEntryMetadata(entry, metadata, false);
         ((MVCCEntry<?, ?>) entry).setTombstone(false);
      }
   }

   /**
    * Same as {@link #setIracMetadata(CacheEntry, IracMetadata, LogSupplier)}  but it stores a "full"
    * {@link PrivateMetadata} instead of {@link IracMetadata}.
    * <p>
    * This method is invoked to set the version from remote site updates. Note that the tombstone is not stored in case
    * of a remove operation.
    *
    * @param entry       The {@link CacheEntry} to update.
    * @param metadata    The {@link PrivateMetadata} to store.
    * @param logSupplier The {@link LogSupplier} to log the {@link PrivateMetadata} and the key.
    */
   public static void setPrivateMetadata(CacheEntry<?, ?> entry, PrivateMetadata metadata, LogSupplier logSupplier) {
      assert metadata.iracMetadata() != null : "[IRAC] Metadata must not be null!";
      assert entry != null;
      assert entry instanceof MVCCEntry;
      Object key = entry.getKey();
      if (entry.isRemoved()) {
         logTombstoneAssociated(key, metadata.iracMetadata(), logSupplier);
         entry.setInternalMetadata(metadata.toTombstone());
         ((MVCCEntry<?, ?>) entry).setTombstone(true);
      } else {
         logIracMetadataAssociated(key, metadata.iracMetadata(), logSupplier);
         entry.setInternalMetadata(metadata.revive());
         ((MVCCEntry<?, ?>) entry).setTombstone(false);
      }
   }

   public static void logUpdateDiscarded(Object key, IracMetadata metadata, LogSupplier logSupplier) {
      if (logSupplier.isTraceEnabled()) {
         logSupplier.getLog().tracef("[IRAC] Update from remote site discarded. Metadata=%s, key=%s", metadata, key);
      }
   }

   private static void logIracMetadataAssociated(Object key, IracMetadata metadata, LogSupplier logSupplier) {
      if (logSupplier.isTraceEnabled()) {
         logSupplier.getLog().tracef("[IRAC] IracMetadata %s associated with key '%s'", metadata, key);
      }
   }

   private static void logTombstoneAssociated(Object key, IracMetadata metadata, LogSupplier logSupplier) {
      if (logSupplier.isTraceEnabled()) {
         logSupplier.getLog().tracef("[IRAC] Store tombstone %s for key '%s'", metadata, key);
      }
   }

   private static void updateCacheEntryMetadata(CacheEntry<?, ?> entry, IracMetadata iracMetadata, boolean tombstone) {
      PrivateMetadata internalMetadata = getBuilder(entry.getInternalMetadata())
            .iracMetadata(iracMetadata)
            .tombstone(tombstone)
            .build();
      entry.setInternalMetadata(internalMetadata);
   }

}
