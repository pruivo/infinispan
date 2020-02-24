package org.infinispan.xsite.irac;

import static org.infinispan.functional.impl.MetaParamsInternalMetadata.getBuilder;

import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.irac.IracVersionGenerator;
import org.infinispan.functional.impl.MetaParamsInternalMetadata;
import org.infinispan.metadata.impl.IracMetaParam;
import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.util.logging.Log;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 10.1
 */
public final class IracUtils {

   private IracUtils() {

   }

   private static void logIracMetadataAssociated(Log log, Object key, IracMetadata metadata) {
      if (log.isDebugEnabled()) {
         log.debugf("[IRAC] IracMetadata %s associated with key '%s'", metadata, key);
      }
   }

   private static void logTombstoneAssociated(Log log, Object key, IracMetadata metadata) {
      if (log.isDebugEnabled()) {
         log.debugf("[IRAC] Store tombstone %s for key '%s'", metadata, key);
      }
   }

   private static void logUpdateDiscarded(Log log, Object key, IracMetadata metadata) {
      if (log.isDebugEnabled()) {
         log.debugf("[IRAC] Update from remote site discarded. Metadata=%s, key=%s", metadata, key);
      }
   }

   private static void discardUpdate(CacheEntry<?, ?> entry, DataWriteCommand command) {
      command.fail(); //this prevents the sending to the backup owners
      entry.setChanged(false); //this prevents the local node to apply the changes.
   }

   private static void updateCacheEntryMetadata(CacheEntry<?, ?> entry, IracMetadata iracMetadata) {
      MetaParamsInternalMetadata.Builder builder = getBuilder(entry.getInternalMetadata());
      builder.add(new IracMetaParam(iracMetadata));
      entry.setInternalMetadata(builder.build());
   }

   public static void updateEntryForRemove(Log log, IracVersionGenerator iracVersionGenerator, CacheEntry<?, ?> entry,
         IracMetadata metadata) {
      final Object key = entry.getKey();
      logTombstoneAssociated(log, key, metadata);
      assert metadata != null : "[IRAC] Metadata must not be null!";
      iracVersionGenerator.storeTombstone(key, metadata);
   }

   public static void updateEntryForWrite(Log log, CacheEntry<?, ?> entry, IracMetadata metadata) {
      final Object key = entry.getKey();
      logIracMetadataAssociated(log, key, metadata);
      assert metadata != null : "[IRAC] Metadata must not be null!";
      updateCacheEntryMetadata(entry, metadata);
   }

   public static void discardUpdate(Log log, CacheEntry<?, ?> entry, DataWriteCommand command, IracMetadata metadata) {
      final Object key = entry.getKey();
      logUpdateDiscarded(log, key, metadata);
      assert metadata != null : "[IRAC] Metadata must not be null!";
      discardUpdate(entry, command);
   }

   public static IracMetadata getIracMetadataFromCommand(WriteCommand command, Object key) {
      return getIracMetadataFromInternalMetadata(command.getInternalMetadata(key));
   }

   public static IracMetadata getIracMetadataFromInternalMetadata(MetaParamsInternalMetadata metadata) {
      return metadata.findMetaParam(IracMetaParam.class)
            .map(IracMetaParam::get)
            .orElse(null);
   }

   public static void updateCommandMetadata(Object key, WriteCommand command, IracMetadata iracMetadata) {
      MetaParamsInternalMetadata.Builder builder = getBuilder(command.getInternalMetadata(key));
      builder.add(new IracMetaParam(iracMetadata));
      command.setInternalMetadata(key, builder.build());
   }

   public static IracMetadata getIracMetadata(CacheEntry<?, ?> entry, IracVersionGenerator versionGenerator) {
      MetaParamsInternalMetadata internalMetadata = entry.getInternalMetadata();
      if (internalMetadata == null) { //new entry!
         return versionGenerator.findTombstone(entry.getKey()).orElse(null);
      }
      // if we don't send anything to remote sites, it doesn't generate any metadata!
      return getIracMetadataFromInternalMetadata(internalMetadata);
   }
}
