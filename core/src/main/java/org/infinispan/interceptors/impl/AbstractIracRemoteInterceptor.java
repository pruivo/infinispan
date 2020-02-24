package org.infinispan.interceptors.impl;

import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.irac.IracEntryVersion;
import org.infinispan.context.InvocationContext;
import org.infinispan.functional.impl.MetaParamsInternalMetadata;
import org.infinispan.metadata.impl.IracMetadata;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 10.1
 */
public abstract class AbstractIracRemoteInterceptor extends AbstractIracInterceptor {

   private static IracMetadata getIracMetadata(CacheEntry<?, ?> entry) {
      MetaParamsInternalMetadata internalMetadata = entry.getInternalMetadata();
      if (internalMetadata == null) { //new entry!
         return null;
      }
      // if we don't send anything to remote sites, it doesn't generate any metadata!
      return getIracMetadataFromInternalMetadata(internalMetadata);
   }

   protected void validateOnPrimary(InvocationContext ctx, DataWriteCommand command, Object rv) {
      final Object key = command.getKey();
      CacheEntry<?, ?> entry = ctx.lookupEntry(key);
      IracMetadata remoteMetadata = getIracMetadataFromCommand(command, key);
      IracMetadata localMetadata = getIracMetadata(entry);

      if (localMetadata == null) {
         localMetadata = iracVersionGenerator.findTombstone(key).orElse(null);
      }

      assert remoteMetadata != null;

      iracVersionGenerator.updateVersion(getSegment(key), remoteMetadata.getVersion());

      if (localMetadata != null) {
         validateAndSetMetadata(entry, command, localMetadata, remoteMetadata);
      } else {
         logIracMetadataAssociated(key, remoteMetadata);
         updateCacheEntryMetadata(entry, remoteMetadata);
      }
   }

   protected void setIracMetadataForOwner(InvocationContext ctx, DataWriteCommand command, Object rv) {
      final Object key = command.getKey();
      IracMetadata metadata = getIracMetadataFromCommand(command, key);
      assert metadata != null;
      iracVersionGenerator.updateVersion(getSegment(key), metadata.getVersion());
      updateCacheEntryMetadata(ctx.lookupEntry(key), metadata);
   }

   private void validateAndSetMetadata(CacheEntry<?, ?> entry, DataWriteCommand command,
         IracMetadata localMetadata, IracMetadata remoteMetadata) {
      if (isTraceEnabled()) {
         getLog().tracef("[IRAC] Comparing local and remote metadata: %s and %s", localMetadata, remoteMetadata);
      }
      IracEntryVersion localVersion = localMetadata.getVersion();
      IracEntryVersion remoteVersion = remoteMetadata.getVersion();
      switch (remoteVersion.compareTo(localVersion)) {
         case CONFLICTING:
            resolveConflict(entry, command, localMetadata, remoteMetadata);
            return;
         case EQUAL:
         case BEFORE:
            logUpdateDiscarded(entry.getKey(), remoteMetadata);
            discardUpdate(command);
            return;
      }
      logIracMetadataAssociated(entry.getKey(), remoteMetadata);
      updateCacheEntryMetadata(entry, remoteMetadata);
   }

   private void logUpdateDiscarded(Object key, IracMetadata metadata) {
      if (isDebugEnabled()) {
         getLog().debugf("[IRAC] Update from remote site discarded. Metadata=%s, key=%s", metadata, key);
      }
   }

   private void resolveConflict(CacheEntry<?, ?> entry, DataWriteCommand command, IracMetadata localMetadata,
         IracMetadata remoteMetadata) {
      //same site? conflict?
      assert !localMetadata.getSite().equals(remoteMetadata.getSite());
      if (localMetadata.getSite().compareTo(remoteMetadata.getSite()) < 0) {
         logUpdateDiscarded(entry.getKey(), remoteMetadata);
         discardUpdate(command);
         return;
      }
      //other site update has priority!
      logIracMetadataAssociated(entry.getKey(), remoteMetadata);
      updateCacheEntryMetadata(entry, remoteMetadata);
      //TODO! this isn't required now but when we allow custom conflict resolution, we need to set it!
      //updateCommandMetadata(entry.getKey(), command, remoteMetadata);
   }

   private static void discardUpdate(DataWriteCommand command) {
      command.fail();
   }
}
