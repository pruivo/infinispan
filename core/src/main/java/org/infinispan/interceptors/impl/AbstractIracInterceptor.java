package org.infinispan.interceptors.impl;

import static org.infinispan.remoting.responses.PrepareResponse.asPrepareResponse;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.irac.IracVersionGenerator;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.context.impl.RemoteTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.distribution.Ownership;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.remoting.responses.PrepareResponse;
import org.infinispan.util.logging.Log;
import org.infinispan.xsite.irac.IracUtils;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 10.1
 */
public abstract class AbstractIracInterceptor extends DDAsyncInterceptor {

   @Inject
   ClusteringDependentLogic clusteringDependentLogic;
   @Inject
   IracVersionGenerator iracVersionGenerator;

   private static boolean isNormalWriteCommand(WriteCommand command) {
      return !command.hasAnyFlag(FlagBitSets.IRAC_UPDATE);
   }

   protected static boolean isIracState(FlagAffectedCommand command) {
      return command.hasAnyFlag(FlagBitSets.IRAC_STATE);
   }

   static LocalTxInvocationContext asLocalTxInvocationContext(InvocationContext ctx) {
      assert ctx.isOriginLocal();
      assert ctx.isInTxScope();
      return (LocalTxInvocationContext) ctx;
   }

   static RemoteTxInvocationContext asRemoteTxInvocationContext(InvocationContext ctx) {
      assert !ctx.isOriginLocal();
      assert ctx.isInTxScope();
      return (RemoteTxInvocationContext) ctx;
   }

   abstract boolean isTraceEnabled();

   abstract Log getLog();

   protected Ownership getOwnership(int segment) {
      return getDistributionInfo(segment).writeOwnership();
   }

   protected Ownership getOwnership(Object key) {
      return getCacheTopology().getDistribution(key).writeOwnership();
   }

   protected DistributionInfo getDistributionInfo(int segment) {
      return getCacheTopology().getSegmentDistribution(segment);
   }

   protected DistributionInfo getDistributionInfo(Object key) {
      return getCacheTopology().getDistribution(key);
   }

   protected boolean isWriteOwner(StreamData data) {
      return getDistributionInfo(data.segment).isWriteOwner();
   }

   protected boolean isPrimaryOwner(StreamData data) {
      return getDistributionInfo(data.segment).isPrimary();
   }

   protected LocalizedCacheTopology getCacheTopology() {
      return clusteringDependentLogic.getCacheTopology();
   }

   protected int getSegment(Object key) {
      return getCacheTopology().getSegment(key);
   }

   protected void setMetadataToCacheEntry(CacheEntry<?, ?> entry, IracMetadata metadata) {
      if (entry.isEvicted()) {
         if (isTraceEnabled()) {
            getLog().tracef("[IRAC] Ignoring evict key: %s", entry.getKey());
         }
         return;
      }
      setIracMetadata(entry, metadata);
   }

   protected final void setIracMetadata(CacheEntry<?, ?> entry, IracMetadata metadata) {
      if (entry.isRemoved()) {
         IracUtils.updateEntryForRemove(getLog(), iracVersionGenerator, entry, metadata);
      } else {
         IracUtils.updateEntryForWrite(getLog(), entry, metadata);
      }
   }

   protected Stream<StreamData> streamKeysFromModifications(List<WriteCommand> mods) {
      return streamKeysFromModifications(mods.stream());
   }

   protected Stream<StreamData> streamKeysFromModifications(WriteCommand[] mods) {
      return streamKeysFromModifications(Stream.of(mods));
   }

   protected Stream<StreamData> streamKeysFromModifications(Stream<WriteCommand> modsStream) {
      return modsStream.filter(AbstractIracInterceptor::isNormalWriteCommand)
            .flatMap(this::streamKeysFromCommand);
   }

   protected Stream<StreamData> streamKeysFromCommand(WriteCommand command) {
      return command.getAffectedKeys().stream().map(key -> new StreamData(key, command, getSegment(key)));
   }

   //used by TotalOrder and Optimistic transaction. Can be moved after total order is removed!
   protected void afterLocalTwoPhasePrepare(InvocationContext ctx, PrepareCommand command, Object rv) {
      if (isTraceEnabled()) {
         getLog().tracef("[IRAC] After successful local prepare for tx %s. Return Value: %s",
               command.getGlobalTransaction(), rv);
      }
      PrepareResponse prepareResponse = asPrepareResponse(rv);
      Iterator<StreamData> iterator = streamKeysFromModifications(command.getModifications()).iterator();
      Map<Integer, IracMetadata> segmentMetadata = new HashMap<>();
      while (iterator.hasNext()) {
         StreamData data = iterator.next();
         IracMetadata metadata;
         if (isPrimaryOwner(data)) {
            metadata = segmentMetadata.computeIfAbsent(data.segment, iracVersionGenerator::generateNewMetadata);
         } else {
            metadata = segmentMetadata.computeIfAbsent(data.segment, prepareResponse::getIracMetadata);
         }
         assert metadata != null : "[IRAC] metadata is null after successful prepare! Data=" + data;
         IracUtils.updateCommandMetadata(data.key, data.command, metadata);
      }
   }

   protected Object afterRemoteTwoPhasePrepare(InvocationContext ctx, PrepareCommand command, Object rv) {
      if (isTraceEnabled()) {
         getLog().tracef("[IRAC] After successful remote prepare for tx %s. Return Value: %s",
               command.getGlobalTransaction(), rv);
      }
      PrepareResponse rsp = PrepareResponse.asPrepareResponse(rv);
      Iterator<StreamData> iterator = streamKeysFromModifications(command.getModifications())
            .filter(this::isPrimaryOwner)
            .distinct()
            .iterator();
      Map<Integer, IracMetadata> segmentMetadata = new HashMap<>();
      while (iterator.hasNext()) {
         StreamData data = iterator.next();
         segmentMetadata.computeIfAbsent(data.segment, iracVersionGenerator::generateNewMetadata);
      }
      rsp.setNewIracMetadata(segmentMetadata);
      if (isTraceEnabled()) {
         getLog().tracef("[IRAC] After successful remote prepare for tx %s. New Return Value: %s",
               command.getGlobalTransaction(), rsp);
      }
      return rsp;
   }

   protected Object onLocalCommitCommand(TxInvocationContext<?> ctx, CommitCommand command) {
      if (isTraceEnabled()) {
         getLog().tracef("[IRAC] On local Commit for tx %s", command.getGlobalTransaction());
      }
      Iterator<StreamData> iterator = streamKeysFromModifications(ctx.getModifications()).iterator();
      while (iterator.hasNext()) {
         StreamData data = iterator.next();
         IracMetadata metadata = IracUtils.getIracMetadataFromCommand(data.command, data.key);

         command.addIracMetadata(data.segment, metadata);
         if (isWriteOwner(data)) {
            setMetadataToCacheEntry(ctx.lookupEntry(data.key), metadata);
         }
      }
      return invokeNext(ctx, command);
   }

   protected Object onRemoteCommitCommand(TxInvocationContext<?> context, CommitCommand command) {
      if (isTraceEnabled()) {
         getLog().tracef("[IRAC] On remote Commit for tx %s", command.getGlobalTransaction());
      }
      RemoteTxInvocationContext ctx = asRemoteTxInvocationContext(context);
      Iterator<StreamData> iterator = streamKeysFromModifications(ctx.getModifications())
            .filter(this::isWriteOwner)
            .iterator();
      while (iterator.hasNext()) {
         StreamData data = iterator.next();
         IracMetadata metadata = command.getIracMetadata(data.segment);
         setMetadataToCacheEntry(ctx.lookupEntry(data.key), metadata);
      }
      return invokeNext(ctx, command);
   }

   static class StreamData {
      final Object key;
      final WriteCommand command;
      final int segment;


      public StreamData(Object key, WriteCommand command, int segment) {
         this.key = key;
         this.command = command;
         this.segment = segment;
      }

      @Override
      public String toString() {
         return "StreamData{" +
                "key=" + key +
                ", command=" + command +
                ", segment=" + segment +
                '}';
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) {
            return true;
         }
         if (o == null || getClass() != o.getClass()) {
            return false;
         }

         StreamData data = (StreamData) o;

         if (segment != data.segment) {
            return false;
         }
         return key.equals(data.key);
      }

      @Override
      public int hashCode() {
         int result = key.hashCode();
         result = 31 * result + segment;
         return result;
      }
   }
}
