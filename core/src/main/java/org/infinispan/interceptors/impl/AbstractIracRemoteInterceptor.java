package org.infinispan.interceptors.impl;

import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.irac.IracEntryVersion;
import org.infinispan.context.InvocationContext;
import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.xsite.irac.IracUtils;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 10.1
 */
public abstract class AbstractIracRemoteInterceptor extends AbstractIracInterceptor {

   protected void validateOnPrimary(InvocationContext ctx, DataWriteCommand command, Object rv) {
      final Object key = command.getKey();
      CacheEntry<?, ?> entry = ctx.lookupEntry(key);
      IracMetadata remoteMetadata = IracUtils.getIracMetadataFromCommand(command, key);
      IracMetadata localMetadata = IracUtils.getIracMetadata(entry, iracVersionGenerator);

      if (localMetadata == null) {
         localMetadata = iracVersionGenerator.findTombstone(key).orElse(null);
      }

      assert remoteMetadata != null;

      iracVersionGenerator.updateVersion(getSegment(key), remoteMetadata.getVersion());

      if (localMetadata != null) {
         validateAndSetMetadata(entry, command, localMetadata, remoteMetadata);
      } else {
         setIracMetadata(entry, remoteMetadata);
      }
   }

   protected void setIracMetadataForOwner(InvocationContext ctx, DataWriteCommand command, Object rv) {
      final Object key = command.getKey();
      IracMetadata metadata = IracUtils.getIracMetadataFromCommand(command, key);
      assert metadata != null;
      iracVersionGenerator.updateVersion(getSegment(key), metadata.getVersion());
      setIracMetadata(ctx.lookupEntry(key), metadata);
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
            IracUtils.discardUpdate(getLog(), entry, command, remoteMetadata);
            return;
      }
      setIracMetadata(entry, remoteMetadata);
   }



   private void resolveConflict(CacheEntry<?, ?> entry, DataWriteCommand command, IracMetadata localMetadata,
         IracMetadata remoteMetadata) {
      if (isTraceEnabled()) {
         getLog().tracef("[IRAC] Conflict found between local and remote metadata: %s and %s", localMetadata,
               remoteMetadata);
      }
      //same site? conflict?
      assert !localMetadata.getSite().equals(remoteMetadata.getSite());
      if (localMetadata.getSite().compareTo(remoteMetadata.getSite()) < 0) {
         IracUtils.discardUpdate(getLog(), entry, command, remoteMetadata);
         return;
      }
      //other site update has priority!
      setIracMetadata(entry, remoteMetadata);
      //TODO! this isn't required now but when we allow custom conflict resolution, we need to set it!
      //updateCommandMetadata(entry.getKey(), command, remoteMetadata);
   }

}
