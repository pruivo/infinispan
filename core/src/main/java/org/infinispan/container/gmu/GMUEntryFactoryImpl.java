package org.infinispan.container.gmu;

import org.infinispan.container.EntryFactoryImpl;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.container.entries.NullMarkerEntry;
import org.infinispan.container.entries.NullMarkerEntryForRemoval;
import org.infinispan.container.entries.SerializableEntry;
import org.infinispan.container.entries.gmu.InternalGMUNullCacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.container.versioning.gmu.GMUVersionGenerator;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.SingleKeyNonTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.GMUMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.Metadatas;
import org.infinispan.transaction.gmu.CommitLog;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import static org.infinispan.transaction.gmu.GMUHelper.toGMUVersionGenerator;

/**
 * @author Pedro Ruivo
 * @author Sebastiano Peluso
 * @since 5.2
 */
public class GMUEntryFactoryImpl extends EntryFactoryImpl {

   private static final Log log = LogFactory.getLog(GMUEntryFactoryImpl.class);
   private CommitLog commitLog;
   private GMUVersionGenerator gmuVersionGenerator;

   public static InternalCacheEntry wrap(Object key, InternalCacheEntry entry, boolean mostRecent,
                                         EntryVersion maxTxVersion, EntryVersion creationVersion,
                                         EntryVersion maxValidVersion) {
      GMUMetadata.GMUBuilder builder;
      if (entry != null) {
         builder = GMUMetadata.fromMetadata(entry.getMetadata());
      } else {
         builder = new GMUMetadata.GMUBuilder();
      }
      builder.creationVersion(creationVersion).maximumValidVersion(maxValidVersion).transactionVersion(maxTxVersion)
            .mostRecent(mostRecent);

      if (entry == null || entry.isNull()) {
         return new InternalGMUNullCacheEntry(key, builder.build());
      }
      InternalCacheEntry wrappedEntry = entry.clone();
      wrappedEntry.setMetadata(builder.build());
      return wrappedEntry;
   }

   @Inject
   public void injectDependencies(CommitLog commitLog, VersionGenerator versionGenerator) {
      this.commitLog = commitLog;
      this.gmuVersionGenerator = toGMUVersionGenerator(versionGenerator);
   }

   public void start() {
      useRepeatableRead = false;
      localModeWriteSkewCheck = false;
   }

   @Override
   protected MVCCEntry createWrappedEntry(Object key, CacheEntry cacheEntry, Metadata providedMetadata,
                                          boolean isForInsert, boolean forRemoval) {
      Metadata metadata;
      Object value;
      if (cacheEntry != null) {
         value = cacheEntry.getValue();
         Metadata entryMetadata = cacheEntry.getMetadata();
         if (providedMetadata != null && entryMetadata != null) {
            metadata = Metadatas.applyVersion(entryMetadata, providedMetadata);
         } else if (providedMetadata == null) {
            metadata = entryMetadata; // take the metadata in memory
         } else {
            metadata = providedMetadata;
         }
      } else {
         value = null;
         metadata = providedMetadata;
      }

      if (value == null && !isForInsert)
         return forRemoval ? new NullMarkerEntryForRemoval(key, metadata)
               : NullMarkerEntry.getInstance();

      return new SerializableEntry(key, value, metadata);
   }

   @Override
   protected InternalCacheEntry getFromContainer(Object key, InvocationContext context) {
      boolean singleRead = context instanceof SingleKeyNonTxInvocationContext;
      boolean remotePrepare = !context.isOriginLocal() && context.isInTxScope();
      boolean remoteRead = !context.isOriginLocal() && !context.isInTxScope();

      EntryVersion versionToRead;
      if (singleRead || remotePrepare) {
         //read the most recent version
         //in the prepare, the value does not matter (it will be written or it is not read)
         //                and the version does not matter either (it will be overwritten)
         versionToRead = null;
      } else {
         versionToRead = context.calculateVersionToRead(gmuVersionGenerator);
      }

      boolean hasAlreadyReadFromThisNode = context.hasAlreadyReadOnThisNode();

      if (context.isInTxScope() && context.isOriginLocal() && !context.hasAlreadyReadOnThisNode()) {
         //firs read on the local node for a transaction. ensure the min version
         EntryVersion transactionVersion = ((TxInvocationContext) context).getTransactionVersion();
         try {
            commitLog.waitForVersion(transactionVersion, -1);
         } catch (InterruptedException e) {
            //ignore...
         }
      }

      EntryVersion maxVersionToRead = hasAlreadyReadFromThisNode ? versionToRead :
            commitLog.getAvailableVersionLessThan(versionToRead);

      EntryVersion mostRecentCommitLogVersion = commitLog.getCurrentVersion();
      InternalCacheEntry entry = container.get(key, new EmbeddedMetadata.Builder().version(maxVersionToRead).build());


      if (remoteRead) {
         GMUMetadata metadata = (GMUMetadata) (entry.getMetadata() != null ? entry.getMetadata() :
                                                     new GMUMetadata.GMUBuilder().build());
         GMUMetadata.GMUBuilder builder = (GMUMetadata.GMUBuilder) metadata.builder();
         if (metadata.maximumValidVersion() == null) {
            builder.maximumValidVersion(mostRecentCommitLogVersion);
         } else {
            builder.maximumValidVersion(commitLog.getEntry(metadata.maximumValidVersion()));
         }
         if (metadata.creationVersion() == null) {
            builder.creationVersion(commitLog.getOldestVersion());
         } else {
            builder.creationVersion(commitLog.getEntry(metadata.creationVersion()));
         }
         entry.setMetadata(builder.build());
      }

      context.addKeyReadInCommand(key, entry);

      if (log.isTraceEnabled()) {
         log.tracef("Retrieved from container %s", entry);
      }

      return entry;
   }
}
