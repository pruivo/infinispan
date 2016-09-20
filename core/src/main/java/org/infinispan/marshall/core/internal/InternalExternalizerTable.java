package org.infinispan.marshall.core.internal;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;

import org.infinispan.atomic.DeltaCompositeKey;
import org.infinispan.atomic.impl.AtomicHashMap;
import org.infinispan.atomic.impl.AtomicHashMapDelta;
import org.infinispan.atomic.impl.ClearOperation;
import org.infinispan.atomic.impl.PutOperation;
import org.infinispan.atomic.impl.RemoveOperation;
import org.infinispan.commands.RemoteCommandsFactory;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.commons.io.ByteBufferImpl;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.Externalizer;
import org.infinispan.commons.marshall.MarshallableFunctionExternalizers;
import org.infinispan.commons.marshall.SerializeFunctionWith;
import org.infinispan.commons.marshall.SerializeWith;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.marshall.exts.EquivalenceExternalizer;
import org.infinispan.commons.util.Immutables;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.ImmortalCacheValue;
import org.infinispan.container.entries.MortalCacheEntry;
import org.infinispan.container.entries.MortalCacheValue;
import org.infinispan.container.entries.TransientCacheEntry;
import org.infinispan.container.entries.TransientCacheValue;
import org.infinispan.container.entries.TransientMortalCacheEntry;
import org.infinispan.container.entries.TransientMortalCacheValue;
import org.infinispan.container.entries.metadata.MetadataImmortalCacheEntry;
import org.infinispan.container.entries.metadata.MetadataImmortalCacheValue;
import org.infinispan.container.entries.metadata.MetadataMortalCacheEntry;
import org.infinispan.container.entries.metadata.MetadataMortalCacheValue;
import org.infinispan.container.entries.metadata.MetadataTransientMortalCacheEntry;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.container.versioning.SimpleClusteredVersion;
import org.infinispan.context.Flag;
import org.infinispan.distribution.ch.impl.DefaultConsistentHash;
import org.infinispan.distribution.ch.impl.ReplicatedConsistentHash;
import org.infinispan.distribution.ch.impl.SyncConsistentHashFactory;
import org.infinispan.distribution.ch.impl.SyncReplicatedConsistentHashFactory;
import org.infinispan.distribution.ch.impl.TopologyAwareSyncConsistentHashFactory;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.filter.AcceptAllKeyValueFilter;
import org.infinispan.filter.CacheFilters;
import org.infinispan.filter.CollectionKeyFilter;
import org.infinispan.filter.KeyFilterAsKeyValueFilter;
import org.infinispan.functional.impl.EntryViews;
import org.infinispan.functional.impl.MetaParams;
import org.infinispan.functional.impl.MetaParamsInternalMetadata;
import org.infinispan.marshall.core.MarshalledEntryImpl;
import org.infinispan.marshall.core.MarshalledValue;
import org.infinispan.marshall.exts.ArrayExternalizer;
import org.infinispan.marshall.exts.CacheEventTypeExternalizer;
import org.infinispan.marshall.exts.CacheRpcCommandExternalizer;
import org.infinispan.marshall.exts.DoubleSummaryStatisticsExternalizer;
import org.infinispan.marshall.exts.EnumSetExternalizer;
import org.infinispan.marshall.exts.IntSummaryStatisticsExternalizer;
import org.infinispan.marshall.exts.ListExternalizer;
import org.infinispan.marshall.exts.LongSummaryStatisticsExternalizer;
import org.infinispan.marshall.exts.MapExternalizer;
import org.infinispan.marshall.exts.MetaParamExternalizers;
import org.infinispan.marshall.exts.OptionalExternalizer;
import org.infinispan.marshall.exts.QueueExternalizers;
import org.infinispan.marshall.exts.ReplicableCommandExternalizer;
import org.infinispan.marshall.exts.SetExternalizer;
import org.infinispan.marshall.exts.SingletonListExternalizer;
import org.infinispan.marshall.exts.UuidExternalizer;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.impl.InternalMetadataImpl;
import org.infinispan.notifications.cachelistener.cluster.ClusterEvent;
import org.infinispan.notifications.cachelistener.cluster.ClusterEventCallable;
import org.infinispan.notifications.cachelistener.cluster.ClusterListenerRemoveCallable;
import org.infinispan.notifications.cachelistener.cluster.ClusterListenerReplicateCallable;
import org.infinispan.notifications.cachelistener.cluster.MultiClusterEventCallable;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverterAsConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterAsKeyValueFilter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterConverterAsKeyValueFilterConverter;
import org.infinispan.notifications.cachelistener.filter.KeyFilterAsCacheEventFilter;
import org.infinispan.notifications.cachelistener.filter.KeyValueFilterAsCacheEventFilter;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.remoting.MIMECacheEntry;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.responses.UnsuccessfulResponse;
import org.infinispan.remoting.responses.UnsureResponse;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.infinispan.remoting.transport.jgroups.JGroupsTopologyAwareAddress;
import org.infinispan.statetransfer.StateChunk;
import org.infinispan.statetransfer.TransactionInfo;
import org.infinispan.stream.StreamMarshalling;
import org.infinispan.stream.impl.intops.IntermediateOperationExternalizer;
import org.infinispan.stream.impl.termop.TerminalOperationExternalizer;
import org.infinispan.topology.CacheJoinInfo;
import org.infinispan.topology.CacheStatusResponse;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.ManagerStatusResponse;
import org.infinispan.topology.PersistentUUID;
import org.infinispan.transaction.xa.DldGlobalTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.recovery.InDoubtTxInfoImpl;
import org.infinispan.transaction.xa.recovery.RecoveryAwareDldGlobalTransaction;
import org.infinispan.transaction.xa.recovery.RecoveryAwareGlobalTransaction;
import org.infinispan.transaction.xa.recovery.SerializableXid;
import org.infinispan.util.IntObjectHashMap;
import org.infinispan.util.IntObjectMap;
import org.infinispan.util.KeyValuePair;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.statetransfer.XSiteState;
import org.jboss.marshalling.util.IdentityIntMap;

final class InternalExternalizerTable {

   private static final Log log = LogFactory.getLog(InternalMarshaller.class);
   private static final boolean trace = log.isTraceEnabled();

   private static final int NOT_FOUND = -1;

   /**
    * Encoding of the internal marshaller
    */
   private final Encoding enc;

   final Map<Class<?>, AdvancedExternalizer> typeToExts = new HashMap<>(512, 0.375f);

   final IntObjectMap<AdvancedExternalizer> idToExts = new IntObjectHashMap<>(128);

   private final GlobalComponentRegistry gcr;
   private final RemoteCommandsFactory cmdFactory;

   InternalExternalizerTable(Encoding enc, GlobalComponentRegistry gcr, RemoteCommandsFactory cmdFactory) {
      this.enc = enc;
      this.gcr = gcr;
      this.cmdFactory = cmdFactory;
   }

   void start() {
      loadInternalMarshallables();
      loadForeignMarshallables(gcr.getGlobalConfiguration());
      if (trace) {
         log.tracef("Type to externalizer map contains: %s", typeToExts);
         log.tracef("Id to externalizer map contains: %s", idToExts);
      }
   }

   static String toStringWithIndex(Object[] arr) {
      StringJoiner sj = new StringJoiner(", ", "[", "]");
      for (int i = 0; i < arr.length; i++)
         sj.add(i + ":" + arr[i]);

      return sj.toString();
   }

   void stop() {
      typeToExts.clear();
      idToExts.clear();
      log.trace("Internal externalizer table has stopped");
   }

   <T> Externalizer<T> findWriteExternalizer(Object obj, ObjectOutput out) throws IOException {
      Class<?> clazz = obj.getClass();
      Externalizer<T> ext = typeToExts.get(clazz);
      if (ext != null) {
         AdvancedExternalizer advExt = (AdvancedExternalizer<?>) ext;
         out.writeByte(InternalIds.PREDEFINED);
         UnsignedNumeric.writeUnsignedInt(out, advExt.getId());
      } else if ((ext = findAnnotatedExternalizer(clazz)) != null) {
         out.writeByte(InternalIds.ANNOTATED);
         out.writeObject(ext.getClass());
      } else {
         out.writeByte(InternalIds.EXTERNAL);
      }

      return ext;
   }

   private <T> Externalizer<T> findAnnotatedExternalizer(Class<?> clazz) {
      try {
         SerializeWith serialAnn = clazz.getAnnotation(SerializeWith.class);
         if (serialAnn != null) {
            return (Externalizer<T>) serialAnn.value().newInstance();
         } else {
            SerializeFunctionWith funcSerialAnn = clazz.getAnnotation(SerializeFunctionWith.class);
            if (funcSerialAnn != null)
               return (Externalizer<T>) funcSerialAnn.value().newInstance();
         }

         return null;
      } catch (Exception e) {
         throw new IllegalArgumentException(String.format(
               "Cannot instantiate externalizer for %s", clazz), e);
      }
   }

   <T> Externalizer<T> findReadExternalizer(ObjectInput in, int type) {
      try {
         switch (type) {
            case InternalIds.PREDEFINED:
               int predefId = UnsignedNumeric.readUnsignedInt(in);
               return idToExts.get(predefId);
            case InternalIds.ANNOTATED:
               Class<? extends Externalizer<T>> clazz =
                     (Class<? extends Externalizer<T>>) in.readObject();
               return clazz.newInstance();
            case InternalIds.EXTERNAL:
               return null;
            default:
               throw new CacheException("Unknown externalizer type: " + type);
         }
      }
      catch (Exception e) {
         // TODO: Update Log.java eventually (not doing yet to avoid need to rebase)
         throw new CacheException("Error reading from input to find externalizer", e);
      }
   }

   MarshallableType marshallable(Object o) {
      Class<?> clazz = o.getClass();
      AdvancedExternalizer ext = typeToExts.get(clazz);
      if (ext != null) {
         // TODO: Remove the need to distinguish primitive externalizers:
         // The need to distinguish comes from the need for types that are
         // externally marshalled to use internal types, including primitives.
         // Primitive marshalling currently requires BytesObjectOutput as
         // parameter, but the output might be different with an external
         // marshaller, e.g. JBoss Marshaller where this might be
         // ExtendedRiverMarshaller and hence won't work.
         // Hence, primitives referenced by external marshallers should be
         // marshalled by the external marshaller directly.
         return isPrimitiveExternalizer(ext)
               ? MarshallableType.PRIMITIVE
               : MarshallableType.PREDEFINED;
      } else if (findAnnotatedExternalizer(clazz) != null) {
         return MarshallableType.ANNOTATED;
      } else {
         return MarshallableType.NOT_MARSHALLABLE;
      }
   }

   private boolean isPrimitiveExternalizer(AdvancedExternalizer ext) {
      return ext instanceof PrimitiveExternalizers.String
            || ext instanceof PrimitiveExternalizers.ByteArray
            || ext instanceof PrimitiveExternalizers.Boolean
            || ext instanceof PrimitiveExternalizers.Byte
            || ext instanceof PrimitiveExternalizers.Char
            || ext instanceof PrimitiveExternalizers.Double
            || ext instanceof PrimitiveExternalizers.Float
            || ext instanceof PrimitiveExternalizers.Int
            || ext instanceof PrimitiveExternalizers.Long
            || ext instanceof PrimitiveExternalizers.Short
            || ext instanceof PrimitiveExternalizers.BooleanArray
            || ext instanceof PrimitiveExternalizers.CharArray
            || ext instanceof PrimitiveExternalizers.DoubleArray
            || ext instanceof PrimitiveExternalizers.FloatArray
            || ext instanceof PrimitiveExternalizers.IntArray
            || ext instanceof PrimitiveExternalizers.LongArray
            || ext instanceof PrimitiveExternalizers.ShortArray
            || ext instanceof PrimitiveExternalizers.ObjectArray;
   }

   private boolean hasExternalizer(Class<?> clazz, IdentityIntMap<Class<?>> col) {
      return col.get(clazz, NOT_FOUND) != NOT_FOUND;
   }

   private void loadInternalMarshallables() {
      StreamingMarshaller marshaller = gcr.getComponent(StreamingMarshaller.class);

      ReplicableCommandExternalizer ext = new ReplicableCommandExternalizer(cmdFactory, gcr);
      addInternalExternalizer(ext);

      addInternalExternalizer(new AcceptAllKeyValueFilter.Externalizer());
      addInternalExternalizer(new ArrayExternalizer());
      addInternalExternalizer(new AtomicHashMap.Externalizer());
      addInternalExternalizer(new AtomicHashMapDelta.Externalizer());
      addInternalExternalizer(new AvailabilityMode.Externalizer());
      addInternalExternalizer(new ByteBufferImpl.Externalizer());
      addInternalExternalizer(new CacheEventConverterAsConverter.Externalizer());
      addInternalExternalizer(new CacheEventFilterAsKeyValueFilter.Externalizer());
      addInternalExternalizer(new CacheEventFilterConverterAsKeyValueFilterConverter.Externalizer());
      addInternalExternalizer(new CacheEventTypeExternalizer());
      addInternalExternalizer(new CacheFilters.CacheFiltersExternalizer());
      addInternalExternalizer(new CacheJoinInfo.Externalizer());
      addInternalExternalizer(new CacheNotFoundResponse.Externalizer());
      addInternalExternalizer(new CacheRpcCommandExternalizer(gcr, ext));
      addInternalExternalizer(new CacheStatusResponse.Externalizer());
      addInternalExternalizer(new CacheTopology.Externalizer());
      addInternalExternalizer(new ClearOperation.Externalizer());
      addInternalExternalizer(new ClusterEvent.Externalizer());
      addInternalExternalizer(new ClusterEventCallable.Externalizer());
      addInternalExternalizer(new ClusterListenerRemoveCallable.Externalizer());
      addInternalExternalizer(new ClusterListenerReplicateCallable.Externalizer());
      addInternalExternalizer(new CollectionKeyFilter.Externalizer());
      addInternalExternalizer(new DefaultConsistentHash.Externalizer());
      addInternalExternalizer(new DeltaCompositeKey.DeltaCompositeKeyExternalizer());
      addInternalExternalizer(new DldGlobalTransaction.Externalizer());
      addInternalExternalizer(new DoubleSummaryStatisticsExternalizer());
      addInternalExternalizer(new EmbeddedMetadata.Externalizer());
      addInternalExternalizer(new EntryViews.ReadWriteSnapshotViewExternalizer());
      addInternalExternalizer(new EnumSetExternalizer());
      addInternalExternalizer(new EquivalenceExternalizer());
      addInternalExternalizer(new ExceptionResponse.Externalizer());
      addInternalExternalizer(new Flag.Externalizer());
      addInternalExternalizer(new GlobalTransaction.Externalizer());
      addInternalExternalizer(new KeyFilterAsCacheEventFilter.Externalizer());
      addInternalExternalizer(new KeyFilterAsKeyValueFilter.Externalizer());
      addInternalExternalizer(new KeyValueFilterAsCacheEventFilter.Externalizer());
      addInternalExternalizer(new ImmortalCacheEntry.Externalizer());
      addInternalExternalizer(new ImmortalCacheValue.Externalizer());
      addInternalExternalizer(new Immutables.ImmutableMapWrapperExternalizer());
      addInternalExternalizer(new Immutables.ImmutableSetWrapperExternalizer());
      addInternalExternalizer(new InDoubtTxInfoImpl.Externalizer());
      addInternalExternalizer(new IntermediateOperationExternalizer());
      addInternalExternalizer(new InternalMetadataImpl.Externalizer());
      addInternalExternalizer(new IntSummaryStatisticsExternalizer());
      addInternalExternalizer(new JGroupsAddress.Externalizer());
      addInternalExternalizer(new JGroupsTopologyAwareAddress.Externalizer());
      addInternalExternalizer(new ListExternalizer());
      addInternalExternalizer(new LongSummaryStatisticsExternalizer());
      addInternalExternalizer(new KeyValuePair.Externalizer());
      addInternalExternalizer(new ManagerStatusResponse.Externalizer());
      addInternalExternalizer(new MapExternalizer());
      addInternalExternalizer(new MarshallableFunctionExternalizers.ConstantLambdaExternalizer());
      addInternalExternalizer(new MarshallableFunctionExternalizers.LambdaWithMetasExternalizer());
      addInternalExternalizer(new MarshallableFunctionExternalizers.SetValueIfEqualsReturnBooleanExternalizer());
      addInternalExternalizer(new MarshalledEntryImpl.Externalizer(marshaller));
      addInternalExternalizer(new MarshalledValue.Externalizer(marshaller));
      addInternalExternalizer(new MetadataImmortalCacheEntry.Externalizer());
      addInternalExternalizer(new MetadataImmortalCacheValue.Externalizer());
      addInternalExternalizer(new MetadataMortalCacheEntry.Externalizer());
      addInternalExternalizer(new MetadataMortalCacheValue.Externalizer());
      addInternalExternalizer(new MetadataTransientMortalCacheEntry.Externalizer());
      addInternalExternalizer(new MetaParamExternalizers.LifespanExternalizer());
      addInternalExternalizer(new MetaParamExternalizers.EntryVersionParamExternalizer());
      addInternalExternalizer(new MetaParamExternalizers.NumericEntryVersionExternalizer());
      addInternalExternalizer(new MetaParams.Externalizer());
      addInternalExternalizer(new MetaParamsInternalMetadata.Externalizer());
      addInternalExternalizer(new MIMECacheEntry.Externalizer()); // new
      addInternalExternalizer(new MortalCacheEntry.Externalizer());
      addInternalExternalizer(new MortalCacheValue.Externalizer());
      addInternalExternalizer(new MultiClusterEventCallable.Externalizer());
      addInternalExternalizer(new MurmurHash3.Externalizer());
      addInternalExternalizer(new NumericVersion.Externalizer());
      addInternalExternalizer(new OptionalExternalizer());
      addInternalExternalizer(new PersistentUUID.Externalizer());
      addInternalExternalizer(new PrimitiveExternalizers.String(enc));
      addInternalExternalizer(new PrimitiveExternalizers.ByteArray());
      addInternalExternalizer(new PrimitiveExternalizers.Boolean());
      addInternalExternalizer(new PrimitiveExternalizers.Byte());
      addInternalExternalizer(new PrimitiveExternalizers.Char());
      addInternalExternalizer(new PrimitiveExternalizers.Double());
      addInternalExternalizer(new PrimitiveExternalizers.Float());
      addInternalExternalizer(new PrimitiveExternalizers.Int());
      addInternalExternalizer(new PrimitiveExternalizers.Long());
      addInternalExternalizer(new PrimitiveExternalizers.Short());
      addInternalExternalizer(new PrimitiveExternalizers.BooleanArray());
      addInternalExternalizer(new PrimitiveExternalizers.CharArray());
      addInternalExternalizer(new PrimitiveExternalizers.DoubleArray());
      addInternalExternalizer(new PrimitiveExternalizers.FloatArray());
      addInternalExternalizer(new PrimitiveExternalizers.IntArray());
      addInternalExternalizer(new PrimitiveExternalizers.LongArray());
      addInternalExternalizer(new PrimitiveExternalizers.ShortArray());
      addInternalExternalizer(new PrimitiveExternalizers.ObjectArray());
      addInternalExternalizer(new PutOperation.Externalizer());
      addInternalExternalizer(new QueueExternalizers());
      addInternalExternalizer(new RecoveryAwareDldGlobalTransaction.Externalizer());
      addInternalExternalizer(new RecoveryAwareGlobalTransaction.Externalizer());
      addInternalExternalizer(new RemoveOperation.Externalizer());
      addInternalExternalizer(new ReplicatedConsistentHash.Externalizer());
      addInternalExternalizer(new SerializableXid.XidExternalizer());
      addInternalExternalizer(new SetExternalizer());
      addInternalExternalizer(new SimpleClusteredVersion.Externalizer());
      addInternalExternalizer(new SingletonListExternalizer());
      addInternalExternalizer(new StateChunk.Externalizer());
      addInternalExternalizer(new StreamMarshalling.StreamMarshallingExternalizer());
      addInternalExternalizer(new SuccessfulResponse.Externalizer());
      addInternalExternalizer(new SyncConsistentHashFactory.Externalizer());
      addInternalExternalizer(new SyncReplicatedConsistentHashFactory.Externalizer());
      addInternalExternalizer(new TerminalOperationExternalizer());
      addInternalExternalizer(new TopologyAwareSyncConsistentHashFactory.Externalizer());
      addInternalExternalizer(new TransactionInfo.Externalizer());
      addInternalExternalizer(new TransientCacheEntry.Externalizer());
      addInternalExternalizer(new TransientCacheValue.Externalizer());
      addInternalExternalizer(new TransientMortalCacheEntry.Externalizer());
      addInternalExternalizer(new TransientMortalCacheValue.Externalizer());
      addInternalExternalizer(new UnsuccessfulResponse.Externalizer());
      addInternalExternalizer(new UnsureResponse.Externalizer());
      addInternalExternalizer(new UuidExternalizer());
      addInternalExternalizer(new XSiteState.XSiteStateExternalizer());

      // ADD NEW INTERNAL EXTERNALIZERS HERE!
   }

   private void addInternalExternalizer(AdvancedExternalizer ext) {
      idToExts.put(ext.getId(), ext);

      Set<Class<?>> subTypes = ext.getTypeClasses();
      for (Class<?> subType : subTypes)
         typeToExts.put(subType, ext);
   }

   private void loadForeignMarshallables(GlobalConfiguration globalCfg) {
      log.trace("Loading user defined externalizers");
      for (Map.Entry<Integer, AdvancedExternalizer<?>> config : globalCfg.serialization().advancedExternalizers().entrySet()) {
         AdvancedExternalizer ext = config.getValue();

         // If no XML or programmatic config, id in annotation is used
         // as long as it's not default one (meaning, user did not set it).
         // If XML or programmatic config in use ignore @Marshalls annotation and use value in config.
         Integer id = ext.getId();
         if (config.getKey() == null && id == null)
            throw new CacheConfigurationException(String.format(
                  "No advanced externalizer identifier set for externalizer %s",
                  ext.getClass().getName()));
         else if (config.getKey() != null)
            id = config.getKey();

         int foreignId = toForeignId(id);
         ForeignExternalizer foreignExt = new ForeignExternalizer(foreignId, ext);
         idToExts.put(foreignId, foreignExt);

         Set<Class> subTypes = ext.getTypeClasses();
         for (Class<?> subType : subTypes) {
            AdvancedExternalizer prev = typeToExts.put(subType, foreignExt);

            if (prev != null && !prev.equals(ext))
               throw log.duplicateExternalizerIdFound(
                     id, subType, prev.getClass().getName(), foreignId);
         }
      }
   }

   private int toForeignId(int id) {
      // Make a large enough number to avoid clashes with other identifiers
      return 0x40000000 | id;
   }

   enum MarshallableType {

      PRIMITIVE,
      INTERNAL,
      PREDEFINED,
      ANNOTATED,
      NOT_MARSHALLABLE;

      boolean isMarshallable() {
         return this != NOT_MARSHALLABLE;
      }

   }

   private final class ForeignExternalizer extends AbstractExternalizer<Object> {

      final int foreignId;
      final AdvancedExternalizer<Object> ext;

      private ForeignExternalizer(int foreignId, AdvancedExternalizer<Object> ext) {
         this.foreignId = foreignId;
         this.ext = ext;
      }

      @Override
      public Integer getId() {
         return foreignId;
      }

      @Override
      public Set<Class<?>> getTypeClasses() {
         return ext.getTypeClasses();
      }

      @Override
      public void writeObject(ObjectOutput output, Object object) throws IOException {
         ext.writeObject(output, object);
      }

      @Override
      public Object readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return ext.readObject(input);
      }

   }

}
