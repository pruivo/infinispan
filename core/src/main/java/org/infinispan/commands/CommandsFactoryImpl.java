package org.infinispan.commands;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import org.infinispan.Cache;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.functional.Mutation;
import org.infinispan.commands.functional.ReadOnlyKeyCommand;
import org.infinispan.commands.functional.ReadOnlyManyCommand;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyValueCommand;
import org.infinispan.commands.functional.ReadWriteManyCommand;
import org.infinispan.commands.functional.ReadWriteManyEntriesCommand;
import org.infinispan.commands.functional.TxReadOnlyKeyCommand;
import org.infinispan.commands.functional.TxReadOnlyManyCommand;
import org.infinispan.commands.functional.WriteOnlyKeyCommand;
import org.infinispan.commands.functional.WriteOnlyKeyValueCommand;
import org.infinispan.commands.functional.WriteOnlyManyCommand;
import org.infinispan.commands.functional.WriteOnlyManyEntriesCommand;
import org.infinispan.commands.irac.IracCleanupKeysCommand;
import org.infinispan.commands.irac.IracMetadataRequestCommand;
import org.infinispan.commands.irac.IracRequestStateCommand;
import org.infinispan.commands.irac.IracStateResponseCommand;
import org.infinispan.commands.irac.IracTombstoneCleanupCommand;
import org.infinispan.commands.irac.IracTombstonePrimaryCheckCommand;
import org.infinispan.commands.irac.IracTombstoneRemoteSiteCheckCommand;
import org.infinispan.commands.irac.IracTombstoneStateResponseCommand;
import org.infinispan.commands.irac.IracUpdateVersionCommand;
import org.infinispan.commands.read.EntrySetCommand;
import org.infinispan.commands.read.GetAllCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.read.KeySetCommand;
import org.infinispan.commands.read.SizeCommand;
import org.infinispan.commands.remote.CheckTransactionRpcCommand;
import org.infinispan.commands.remote.ClusteredGetAllCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.commands.remote.recovery.CompleteTransactionCommand;
import org.infinispan.commands.remote.recovery.GetInDoubtTransactionsCommand;
import org.infinispan.commands.remote.recovery.GetInDoubtTxInfoCommand;
import org.infinispan.commands.remote.recovery.TxCompletionNotificationCommand;
import org.infinispan.commands.statetransfer.ConflictResolutionStartCommand;
import org.infinispan.commands.statetransfer.StateResponseCommand;
import org.infinispan.commands.statetransfer.StateTransferCancelCommand;
import org.infinispan.commands.statetransfer.StateTransferGetListenersCommand;
import org.infinispan.commands.statetransfer.StateTransferGetTransactionsCommand;
import org.infinispan.commands.statetransfer.StateTransferStartCommand;
import org.infinispan.commands.triangle.BackupNoopCommand;
import org.infinispan.commands.triangle.MultiEntriesFunctionalBackupWriteCommand;
import org.infinispan.commands.triangle.MultiKeyFunctionalBackupWriteCommand;
import org.infinispan.commands.triangle.PutMapBackupWriteCommand;
import org.infinispan.commands.triangle.SingleKeyBackupWriteCommand;
import org.infinispan.commands.triangle.SingleKeyFunctionalBackupWriteCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.tx.VersionedCommitCommand;
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.commands.write.BackupMultiKeyAckCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.ComputeCommand;
import org.infinispan.commands.write.ComputeIfAbsentCommand;
import org.infinispan.commands.write.EvictCommand;
import org.infinispan.commands.write.ExceptionAckCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.InvalidateL1Command;
import org.infinispan.commands.write.IracPutKeyValueCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.RemoveExpiredCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.ValueMatcher;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.marshall.Externalizer;
import org.infinispan.commons.marshall.LambdaExternalizer;
import org.infinispan.commons.marshall.SerializeFunctionWith;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.tx.XidImpl;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.commons.util.IntSet;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.XSiteStateTransferMode;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.irac.IracEntryVersion;
import org.infinispan.container.versioning.irac.IracTombstoneInfo;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.encoding.DataConversion;
import org.infinispan.expiration.impl.TouchCommand;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.functional.EntryView.ReadEntryView;
import org.infinispan.functional.EntryView.ReadWriteEntryView;
import org.infinispan.functional.EntryView.WriteEntryView;
import org.infinispan.functional.impl.Params;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.marshall.core.GlobalMarshaller;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.notifications.cachelistener.cluster.ClusterEvent;
import org.infinispan.notifications.cachelistener.cluster.MultiClusterEventCommand;
import org.infinispan.reactive.publisher.impl.DeliveryGuarantee;
import org.infinispan.reactive.publisher.impl.commands.batch.CancelPublisherCommand;
import org.infinispan.reactive.publisher.impl.commands.batch.InitialPublisherCommand;
import org.infinispan.reactive.publisher.impl.commands.batch.NextPublisherCommand;
import org.infinispan.reactive.publisher.impl.commands.reduction.ReductionPublisherRequestCommand;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.StateChunk;
import org.infinispan.telemetry.InfinispanTelemetry;
import org.infinispan.telemetry.impl.CacheSpanAttribute;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.ByteString;
import org.infinispan.xsite.SingleXSiteRpcCommand;
import org.infinispan.xsite.commands.XSiteAmendOfflineStatusCommand;
import org.infinispan.xsite.commands.XSiteAutoTransferStatusCommand;
import org.infinispan.xsite.commands.XSiteBringOnlineCommand;
import org.infinispan.xsite.commands.XSiteOfflineStatusCommand;
import org.infinispan.xsite.commands.XSiteSetStateTransferModeCommand;
import org.infinispan.xsite.commands.XSiteStateTransferCancelSendCommand;
import org.infinispan.xsite.commands.XSiteStateTransferClearStatusCommand;
import org.infinispan.xsite.commands.XSiteStateTransferFinishReceiveCommand;
import org.infinispan.xsite.commands.XSiteStateTransferFinishSendCommand;
import org.infinispan.xsite.commands.XSiteStateTransferRestartSendingCommand;
import org.infinispan.xsite.commands.XSiteStateTransferStartReceiveCommand;
import org.infinispan.xsite.commands.XSiteStateTransferStartSendCommand;
import org.infinispan.xsite.commands.XSiteStateTransferStatusRequestCommand;
import org.infinispan.xsite.commands.XSiteStatusCommand;
import org.infinispan.xsite.commands.XSiteTakeOfflineCommand;
import org.infinispan.xsite.commands.remote.IracClearKeysRequest;
import org.infinispan.xsite.commands.remote.IracPutManyRequest;
import org.infinispan.xsite.commands.remote.IracTombstoneCheckRequest;
import org.infinispan.xsite.commands.remote.IracTouchKeyRequest;
import org.infinispan.xsite.commands.remote.XSiteStatePushRequest;
import org.infinispan.xsite.commands.remote.XSiteStateTransferControlRequest;
import org.infinispan.xsite.irac.IracManagerKeyInfo;
import org.infinispan.xsite.statetransfer.XSiteState;
import org.infinispan.xsite.statetransfer.XSiteStatePushCommand;
import org.reactivestreams.Publisher;

/**
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @author Sanne Grinovero &lt;sanne@hibernate.org&gt; (C) 2011 Red Hat Inc.
 * @since 4.0
 */
@Scope(Scopes.NAMED_CACHE)
public class CommandsFactoryImpl implements CommandsFactory {

   @Inject ClusteringDependentLogic clusteringDependentLogic;
   @Inject Configuration configuration;
   @Inject ComponentRef<Cache<Object, Object>> cache;
   @Inject ComponentRegistry componentRegistry;
   @Inject
   InfinispanTelemetry telemetry;
   @Inject
   CacheSpanAttribute cacheSpanAttribute;
   @Inject @ComponentName(KnownComponentNames.INTERNAL_MARSHALLER)
   StreamingMarshaller marshaller;

   private ByteString cacheName;
   private boolean transactional;

   @Start
   // needs to happen early on
   public void start() {
      cacheName = ByteString.fromString(cache.wired().getName());
      transactional = configuration.transaction().transactionMode().isTransactional();
   }

   @Override
   public PutKeyValueCommand buildPutKeyValueCommand(Object key, Object value, int segment, Metadata metadata,
                                                     long flagsBitSet, boolean returnEntry) {
      boolean reallyTransactional = transactional && !EnumUtil.containsAny(flagsBitSet, FlagBitSets.PUT_FOR_EXTERNAL_READ);
      return injectTracing(new PutKeyValueCommand(key, value, false, returnEntry, metadata, segment, flagsBitSet, generateUUID(reallyTransactional)));
   }

   @Override
   public RemoveCommand buildRemoveCommand(Object key, Object value, int segment, long flagsBitSet, boolean returnEntry) {
      return injectTracing(new RemoveCommand(key, value, returnEntry, segment, flagsBitSet, generateUUID(transactional)));
   }

   @Override
   public InvalidateCommand buildInvalidateCommand(long flagsBitSet, Object... keys) {
      // StateConsumerImpl always uses non-tx invalidation
      return injectTracing(new InvalidateCommand(flagsBitSet, generateUUID(false), keys));
   }

   @Override
   public InvalidateCommand buildInvalidateFromL1Command(long flagsBitSet, Collection<Object> keys) {
      // StateConsumerImpl always uses non-tx invalidation
      return injectTracing(new InvalidateL1Command(flagsBitSet, keys, generateUUID(transactional)));
   }

   @Override
   public InvalidateCommand buildInvalidateFromL1Command(Address origin, long flagsBitSet, Collection<Object> keys) {
      // L1 invalidation is always non-transactional
      return injectTracing(new InvalidateL1Command(origin, flagsBitSet, keys, generateUUID(false)));
   }

   @Override
   public RemoveExpiredCommand buildRemoveExpiredCommand(Object key, Object value, int segment, Long lifespan,
         long flagsBitSet) {
      return injectTracing(new RemoveExpiredCommand(key, value, lifespan, false, segment, flagsBitSet,
            generateUUID(false)));
   }

   @Override
   public RemoveExpiredCommand buildRemoveExpiredCommand(Object key, Object value, int segment, long flagsBitSet) {
      return injectTracing(new RemoveExpiredCommand(key, value, null, true, segment, flagsBitSet,
            generateUUID(false)));
   }

   @Override
   public ReplaceCommand buildReplaceCommand(Object key, Object oldValue, Object newValue, int segment,
                                             Metadata metadata, long flagsBitSet, boolean returnEntry) {
      return injectTracing(new ReplaceCommand(key, oldValue, newValue, returnEntry, metadata, segment, flagsBitSet, generateUUID(transactional)));
   }

   @Override
   public ComputeCommand buildComputeCommand(Object key, BiFunction mappingFunction, boolean computeIfPresent, int segment, Metadata metadata, long flagsBitSet) {
      return init(new ComputeCommand(key, mappingFunction, computeIfPresent, segment, flagsBitSet, generateUUID(transactional), metadata));
   }

   @Override
   public ComputeIfAbsentCommand buildComputeIfAbsentCommand(Object key, Function mappingFunction, int segment, Metadata metadata, long flagsBitSet) {
      return init(new ComputeIfAbsentCommand(key, mappingFunction, segment, flagsBitSet, generateUUID(transactional), metadata));
   }

   @Override
   public SizeCommand buildSizeCommand(IntSet segments, long flagsBitSet) {
      return injectTracing(new SizeCommand(cacheName, segments, flagsBitSet));
   }

   @Override
   public KeySetCommand buildKeySetCommand(long flagsBitSet) {
      return injectTracing(new KeySetCommand<>(flagsBitSet));
   }

   @Override
   public EntrySetCommand buildEntrySetCommand(long flagsBitSet) {
      return injectTracing(new EntrySetCommand<>(flagsBitSet));
   }

   @Override
   public GetKeyValueCommand buildGetKeyValueCommand(Object key, int segment, long flagsBitSet) {
      return injectTracing(new GetKeyValueCommand(key, segment, flagsBitSet));
   }

   @Override
   public GetAllCommand buildGetAllCommand(Collection<?> keys, long flagsBitSet, boolean returnEntries) {
      return injectTracing(new GetAllCommand(keys, flagsBitSet, returnEntries));
   }

   @Override
   public PutMapCommand buildPutMapCommand(Map<?, ?> map, Metadata metadata, long flagsBitSet) {
      return injectTracing(new PutMapCommand(map, metadata, flagsBitSet, generateUUID(transactional)));
   }

   @Override
   public ClearCommand buildClearCommand(long flagsBitSet) {
      return injectTracing(new ClearCommand(flagsBitSet));
   }

   @Override
   public EvictCommand buildEvictCommand(Object key, int segment, long flagsBitSet) {
      return injectTracing(new EvictCommand(key, segment, flagsBitSet, generateUUID(transactional)));
   }

   @Override
   public PrepareCommand buildPrepareCommand(GlobalTransaction gtx, List<WriteCommand> modifications, boolean onePhaseCommit) {
      return injectTracing(new PrepareCommand(cacheName, gtx, modifications, onePhaseCommit));
   }

   @Override
   public VersionedPrepareCommand buildVersionedPrepareCommand(GlobalTransaction gtx, List<WriteCommand> modifications, boolean onePhase) {
      return injectTracing(new VersionedPrepareCommand(cacheName, gtx, modifications, onePhase));
   }

   @Override
   public CommitCommand buildCommitCommand(GlobalTransaction gtx) {
      return injectTracing(new CommitCommand(cacheName, gtx));
   }

   @Override
   public VersionedCommitCommand buildVersionedCommitCommand(GlobalTransaction gtx) {
      return injectTracing(new VersionedCommitCommand(cacheName, gtx));
   }

   @Override
   public RollbackCommand buildRollbackCommand(GlobalTransaction gtx) {
      return injectTracing(new RollbackCommand(cacheName, gtx));
   }

   @Override
   public SingleRpcCommand buildSingleRpcCommand(VisitableCommand call) {
      return injectTracing(new SingleRpcCommand(cacheName, call));
   }

   @Override
   public ClusteredGetCommand buildClusteredGetCommand(Object key, Integer segment, long flagsBitSet) {
      return injectTracing(new ClusteredGetCommand(key, cacheName, segment, flagsBitSet));
   }

   /**
    * @param isRemote true if the command is deserialized and is executed remote.
    */
   @Override
   public void initializeReplicableCommand(ReplicableCommand c, boolean isRemote) {
      if (c == null) {
         return;
      }

      if (c instanceof InitializableCommand) {
         ((InitializableCommand) c).init(componentRegistry, isRemote);
      }
      c.updateTraceData(cacheSpanAttribute);
   }

   private <T extends VisitableCommand & TracedCommand> T init(T cmd) {
      cmd.init(componentRegistry);
      return injectTracing(cmd);
   }

   @Override
   public LockControlCommand buildLockControlCommand(Collection<?> keys, long flagsBitSet, GlobalTransaction gtx) {
      return injectTracing(new LockControlCommand(keys, cacheName, flagsBitSet, gtx));
   }

   @Override
   public LockControlCommand buildLockControlCommand(Object key, long flagsBitSet, GlobalTransaction gtx) {
      return injectTracing(new LockControlCommand(key, cacheName, flagsBitSet, gtx));
   }

   @Override
   public LockControlCommand buildLockControlCommand(Collection<?> keys, long flagsBitSet) {
      return injectTracing(new LockControlCommand(keys, cacheName, flagsBitSet, null));
   }

   @Override
   public ConflictResolutionStartCommand buildConflictResolutionStartCommand(int topologyId, IntSet segments) {
      return injectTracing(new ConflictResolutionStartCommand(cacheName, topologyId, segments));
   }

   @Override
   public StateTransferCancelCommand buildStateTransferCancelCommand(int topologyId, IntSet segments) {
      return injectTracing(new StateTransferCancelCommand(cacheName, topologyId, segments));
   }

   @Override
   public StateTransferGetListenersCommand buildStateTransferGetListenersCommand(int topologyId) {
      return injectTracing(new StateTransferGetListenersCommand(cacheName, topologyId));
   }

   @Override
   public StateTransferGetTransactionsCommand buildStateTransferGetTransactionsCommand(int topologyId, IntSet segments) {
      return injectTracing(new StateTransferGetTransactionsCommand(cacheName, topologyId, segments));
   }

   @Override
   public StateTransferStartCommand buildStateTransferStartCommand(int topologyId, IntSet segments) {
      return injectTracing(new StateTransferStartCommand(cacheName, topologyId, segments));
   }

   @Override
   public StateResponseCommand buildStateResponseCommand(int topologyId, Collection<StateChunk> stateChunks, boolean applyState) {
      return injectTracing(new StateResponseCommand(cacheName, topologyId, stateChunks, applyState));
   }

   @Override
   public String getCacheName() {
      return cacheName.toString();
   }

   @Override
   public GetInDoubtTransactionsCommand buildGetInDoubtTransactionsCommand() {
      return injectTracing(new GetInDoubtTransactionsCommand(cacheName));
   }

   @Override
   public TxCompletionNotificationCommand buildTxCompletionNotificationCommand(XidImpl xid, GlobalTransaction globalTransaction) {
      return injectTracing(new TxCompletionNotificationCommand(xid, globalTransaction, cacheName));
   }

   @Override
   public TxCompletionNotificationCommand buildTxCompletionNotificationCommand(long internalId) {
      return injectTracing(new TxCompletionNotificationCommand(internalId, cacheName));
   }

   @Override
   public GetInDoubtTxInfoCommand buildGetInDoubtTxInfoCommand() {
      return injectTracing(new GetInDoubtTxInfoCommand(cacheName));
   }

   @Override
   public CompleteTransactionCommand buildCompleteTransactionCommand(XidImpl xid, boolean commit) {
      return injectTracing(new CompleteTransactionCommand(cacheName, xid, commit));
   }

   @Override
   public XSiteStateTransferCancelSendCommand buildXSiteStateTransferCancelSendCommand(String siteName) {
      return injectTracing(new XSiteStateTransferCancelSendCommand(cacheName, siteName));
   }

   @Override
   public XSiteStateTransferClearStatusCommand buildXSiteStateTransferClearStatusCommand() {
      return injectTracing(new XSiteStateTransferClearStatusCommand(cacheName));
   }

   @Override
   public XSiteStateTransferFinishReceiveCommand buildXSiteStateTransferFinishReceiveCommand(String siteName) {
      return injectTracing(new XSiteStateTransferFinishReceiveCommand(cacheName, siteName));
   }

   @Override
   public XSiteStateTransferFinishSendCommand buildXSiteStateTransferFinishSendCommand(String siteName, boolean statusOk) {
      return injectTracing(new XSiteStateTransferFinishSendCommand(cacheName, siteName, statusOk));
   }

   @Override
   public XSiteStateTransferRestartSendingCommand buildXSiteStateTransferRestartSendingCommand(String siteName, int topologyId) {
      return injectTracing(new XSiteStateTransferRestartSendingCommand(cacheName, siteName, topologyId));
   }

   @Override
   public XSiteStateTransferStartReceiveCommand buildXSiteStateTransferStartReceiveCommand(String siteName) {
      return injectTracing(new XSiteStateTransferStartReceiveCommand(cacheName, siteName));
   }

   @Override
   public XSiteStateTransferStartSendCommand buildXSiteStateTransferStartSendCommand(String siteName, int topologyId) {
      return injectTracing(new XSiteStateTransferStartSendCommand(cacheName, siteName, topologyId));
   }

   @Override
   public XSiteStateTransferStatusRequestCommand buildXSiteStateTransferStatusRequestCommand() {
      return injectTracing(new XSiteStateTransferStatusRequestCommand(cacheName));
   }

   @Override
   public XSiteStateTransferControlRequest buildXSiteStateTransferControlRequest(boolean startReceiving) {
      return injectTracing(new XSiteStateTransferControlRequest(cacheName, startReceiving));
   }

   @Override
   public XSiteAmendOfflineStatusCommand buildXSiteAmendOfflineStatusCommand(String siteName, Integer afterFailures, Long minTimeToWait) {
      return injectTracing(new XSiteAmendOfflineStatusCommand(cacheName, siteName, afterFailures, minTimeToWait));
   }

   @Override
   public XSiteBringOnlineCommand buildXSiteBringOnlineCommand(String siteName) {
      return injectTracing(new XSiteBringOnlineCommand(cacheName, siteName));
   }

   @Override
   public XSiteOfflineStatusCommand buildXSiteOfflineStatusCommand(String siteName) {
      return injectTracing(new XSiteOfflineStatusCommand(cacheName, siteName));
   }

   @Override
   public XSiteStatusCommand buildXSiteStatusCommand() {
      return injectTracing(new XSiteStatusCommand(cacheName));
   }

   @Override
   public XSiteTakeOfflineCommand buildXSiteTakeOfflineCommand(String siteName) {
      return injectTracing(new XSiteTakeOfflineCommand(cacheName, siteName));
   }

   @Override
   public XSiteStatePushCommand buildXSiteStatePushCommand(XSiteState[] chunk) {
      return injectTracing(new XSiteStatePushCommand(cacheName, chunk));
   }

   @Override
   public SingleXSiteRpcCommand buildSingleXSiteRpcCommand(VisitableCommand command) {
      return injectTracing(new SingleXSiteRpcCommand(cacheName, command));
   }

   @Override
   public GetCacheEntryCommand buildGetCacheEntryCommand(Object key, int segment, long flagsBitSet) {
      return injectTracing(new GetCacheEntryCommand(key, segment, flagsBitSet));
   }

   @Override
   public ClusteredGetAllCommand buildClusteredGetAllCommand(List<?> keys, long flagsBitSet, GlobalTransaction gtx) {
      return injectTracing(new ClusteredGetAllCommand(cacheName, keys, flagsBitSet, gtx));
   }

   private CommandInvocationId generateUUID(boolean tx) {
      if (tx) {
         return CommandInvocationId.DUMMY_INVOCATION_ID;
      } else {
         return CommandInvocationId.generateId(clusteringDependentLogic.getAddress());
      }
   }

   @Override
   public <K, V, R> ReadOnlyKeyCommand<K, V, R> buildReadOnlyKeyCommand(Object key, Function<ReadEntryView<K, V>, R> f,
         int segment, Params params, DataConversion keyDataConversion, DataConversion valueDataConversion) {
      return init(new ReadOnlyKeyCommand<>(key, f, segment, params, keyDataConversion, valueDataConversion));
   }

   @Override
   public <K, V, R> ReadOnlyManyCommand<K, V, R> buildReadOnlyManyCommand(Collection<?> keys, Function<ReadEntryView<K, V>, R> f, Params params, DataConversion keyDataConversion, DataConversion valueDataConversion) {
      return init(new ReadOnlyManyCommand<>(keys, f, params, keyDataConversion, valueDataConversion));
   }

   @Override
   public <K, V, T, R> ReadWriteKeyValueCommand<K, V, T, R> buildReadWriteKeyValueCommand(Object key, Object argument,
         BiFunction<T, ReadWriteEntryView<K, V>, R> f, int segment, Params params, DataConversion keyDataConversion, DataConversion valueDataConversion) {
      return init(new ReadWriteKeyValueCommand<>(key, argument, f, segment, generateUUID(transactional), getValueMatcher(f),
            params, keyDataConversion, valueDataConversion));
   }

   @Override
   public <K, V, R> ReadWriteKeyCommand<K, V, R> buildReadWriteKeyCommand(Object key,
         Function<ReadWriteEntryView<K, V>, R> f, int segment, Params params, DataConversion keyDataConversion,
         DataConversion valueDataConversion) {
      return init(new ReadWriteKeyCommand<>(key, f, segment, generateUUID(transactional), getValueMatcher(f), params, keyDataConversion, valueDataConversion));
   }

   @Override
   public <K, V, R> ReadWriteManyCommand<K, V, R> buildReadWriteManyCommand(Collection<?> keys, Function<ReadWriteEntryView<K, V>, R> f, Params params, DataConversion keyDataConversion, DataConversion valueDataConversion) {
      return init(new ReadWriteManyCommand<>(keys, f, params, generateUUID(transactional), keyDataConversion, valueDataConversion));
   }

   @Override
   public <K, V, T, R> ReadWriteManyEntriesCommand<K, V, T, R> buildReadWriteManyEntriesCommand(Map<?, ?> entries, BiFunction<T, ReadWriteEntryView<K, V>, R> f, Params params, DataConversion keyDataConversion, DataConversion valueDataConversion) {
      return init(new ReadWriteManyEntriesCommand<>(entries, f, params, generateUUID(transactional), keyDataConversion, valueDataConversion));
   }

   @Override
   public <K, V> WriteOnlyKeyCommand<K, V> buildWriteOnlyKeyCommand(
         Object key, Consumer<WriteEntryView<K, V>> f, int segment, Params params, DataConversion keyDataConversion, DataConversion valueDataConversion) {
      return init(new WriteOnlyKeyCommand<>(key, f, segment, generateUUID(transactional), getValueMatcher(f), params, keyDataConversion, valueDataConversion));
   }

   @Override
   public <K, V, T> WriteOnlyKeyValueCommand<K, V, T> buildWriteOnlyKeyValueCommand(Object key, Object argument, BiConsumer<T, WriteEntryView<K, V>> f,
         int segment, Params params, DataConversion keyDataConversion, DataConversion valueDataConversion) {
      return init(new WriteOnlyKeyValueCommand<>(key, argument, f, segment, generateUUID(transactional), getValueMatcher(f), params, keyDataConversion, valueDataConversion));
   }

   @Override
   public <K, V> WriteOnlyManyCommand<K, V> buildWriteOnlyManyCommand(Collection<?> keys, Consumer<WriteEntryView<K, V>> f, Params params, DataConversion keyDataConversion, DataConversion valueDataConversion) {
      return init(new WriteOnlyManyCommand<>(keys, f, params, generateUUID(transactional), keyDataConversion, valueDataConversion));
   }

   @Override
   public <K, V, T> WriteOnlyManyEntriesCommand<K, V, T> buildWriteOnlyManyEntriesCommand(
         Map<?, ?> arguments, BiConsumer<T, WriteEntryView<K, V>> f, Params params, DataConversion keyDataConversion, DataConversion valueDataConversion) {
      return init(new WriteOnlyManyEntriesCommand<>(arguments, f, params, generateUUID(transactional), keyDataConversion, valueDataConversion));
   }

   @Override
   public <K, V, R> TxReadOnlyKeyCommand<K, V, R> buildTxReadOnlyKeyCommand(Object key, Function<ReadEntryView<K, V>, R> f, List<Mutation<K, V, ?>> mutations, int segment, Params params, DataConversion keyDataConversion, DataConversion valueDataConversion) {
      return init(new TxReadOnlyKeyCommand<>(key, f, mutations, segment, params, keyDataConversion, valueDataConversion));
   }

   @Override
   public <K, V, R> TxReadOnlyManyCommand<K, V, R> buildTxReadOnlyManyCommand(Collection<?> keys, List<List<Mutation<K, V, ?>>> mutations,
                                                                              Params params, DataConversion keyDataConversion,
                                                                              DataConversion valueDataConversion) {
      return init(new TxReadOnlyManyCommand<>(keys, mutations, params, keyDataConversion, valueDataConversion));
   }

   @Override
   public BackupMultiKeyAckCommand buildBackupMultiKeyAckCommand(long id, int segment, int topologyId) {
      return injectTracing(new BackupMultiKeyAckCommand(cacheName, id, segment, topologyId));
   }

   @Override
   public ExceptionAckCommand buildExceptionAckCommand(long id, Throwable throwable, int topologyId) {
      return injectTracing(new ExceptionAckCommand(cacheName, id, throwable, topologyId));
   }

   private ValueMatcher getValueMatcher(Object o) {
      SerializeFunctionWith ann = o.getClass().getAnnotation(SerializeFunctionWith.class);
      if (ann != null) {
         return ValueMatcher.valueOf(ann.valueMatcher().toString());
      }

      Externalizer ext = ((GlobalMarshaller) marshaller).findExternalizerFor(o);
      if (ext instanceof LambdaExternalizer) {
         return ValueMatcher.valueOf(((LambdaExternalizer) ext).valueMatcher(o).toString());
      }

      return ValueMatcher.MATCH_ALWAYS;
   }

   @Override
   public SingleKeyBackupWriteCommand buildSingleKeyBackupWriteCommand() {
      return injectTracing(new SingleKeyBackupWriteCommand(cacheName));
   }

   @Override
   public SingleKeyFunctionalBackupWriteCommand buildSingleKeyFunctionalBackupWriteCommand() {
      return injectTracing(new SingleKeyFunctionalBackupWriteCommand(cacheName));
   }

   @Override
   public PutMapBackupWriteCommand buildPutMapBackupWriteCommand() {
      return injectTracing(new PutMapBackupWriteCommand(cacheName));
   }

   @Override
   public MultiEntriesFunctionalBackupWriteCommand buildMultiEntriesFunctionalBackupWriteCommand() {
      return injectTracing(new MultiEntriesFunctionalBackupWriteCommand(cacheName));
   }

   @Override
   public MultiKeyFunctionalBackupWriteCommand buildMultiKeyFunctionalBackupWriteCommand() {
      return injectTracing(new MultiKeyFunctionalBackupWriteCommand(cacheName));
   }

   @Override
   public BackupNoopCommand buildBackupNoopCommand() {
      return injectTracing(new BackupNoopCommand(cacheName));
   }

   @Override
   public <K, R> ReductionPublisherRequestCommand<K> buildKeyReductionPublisherCommand(boolean parallelStream,
         DeliveryGuarantee deliveryGuarantee, IntSet segments, Set<K> keys, Set<K> excludedKeys, long explicitFlags,
         Function<? super Publisher<K>, ? extends CompletionStage<R>> transformer,
         Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
      return injectTracing(new ReductionPublisherRequestCommand<>(cacheName, parallelStream, deliveryGuarantee, segments, keys, excludedKeys,
            explicitFlags, false, transformer, finalizer));
   }

   @Override
   public <K, V, R> ReductionPublisherRequestCommand<K> buildEntryReductionPublisherCommand(boolean parallelStream,
         DeliveryGuarantee deliveryGuarantee, IntSet segments, Set<K> keys, Set<K> excludedKeys, long explicitFlags,
         Function<? super Publisher<CacheEntry<K, V>>, ? extends CompletionStage<R>> transformer,
         Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
      return injectTracing(new ReductionPublisherRequestCommand<>(cacheName, parallelStream, deliveryGuarantee, segments, keys, excludedKeys,
            explicitFlags, true, transformer, finalizer));
   }

   @Override
   public <K, I, R> InitialPublisherCommand<K, I, R> buildInitialPublisherCommand(String requestId, DeliveryGuarantee deliveryGuarantee,
                                                                                  int batchSize, IntSet segments, Set<K> keys, Set<K> excludedKeys, long explicitFlags, boolean entryStream,
                                                                                  boolean trackKeys, Function<? super Publisher<I>, ? extends Publisher<R>> transformer) {
      return injectTracing(new InitialPublisherCommand<>(cacheName, requestId, deliveryGuarantee, batchSize, segments, keys,
            excludedKeys, explicitFlags, entryStream, trackKeys, transformer));
   }

   @Override
   public NextPublisherCommand buildNextPublisherCommand(String requestId) {
      return injectTracing(new NextPublisherCommand(cacheName, requestId));
   }

   @Override
   public CancelPublisherCommand buildCancelPublisherCommand(String requestId) {
      return injectTracing(new CancelPublisherCommand(cacheName, requestId));
   }

   @Override
   public <K, V> MultiClusterEventCommand<K, V> buildMultiClusterEventCommand(Map<UUID, Collection<ClusterEvent<K, V>>> events) {
      return injectTracing(new MultiClusterEventCommand<>(cacheName, events));
   }

   @Override
   public CheckTransactionRpcCommand buildCheckTransactionRpcCommand(Collection<GlobalTransaction> globalTransactions) {
      return injectTracing(new CheckTransactionRpcCommand(cacheName, globalTransactions));
   }

   @Override
   public TouchCommand buildTouchCommand(Object key, int segment, boolean touchEvenIfExpired, long flagBitSet) {
      return injectTracing(new TouchCommand(key, segment, flagBitSet, touchEvenIfExpired));
   }

   @Override
   public IracClearKeysRequest buildIracClearKeysCommand() {
      return injectTracing(new IracClearKeysRequest(cacheName));
   }

   @Override
   public IracCleanupKeysCommand buildIracCleanupKeyCommand(Collection<IracManagerKeyInfo> state) {
      return injectTracing(new IracCleanupKeysCommand(cacheName, state));
   }

   @Override
   public IracTombstoneCleanupCommand buildIracTombstoneCleanupCommand(int maxCapacity) {
      return injectTracing(new IracTombstoneCleanupCommand(cacheName, maxCapacity));
   }

   @Override
   public IracMetadataRequestCommand buildIracMetadataRequestCommand(int segment, IracEntryVersion versionSeen) {
      return injectTracing(new IracMetadataRequestCommand(cacheName, segment, versionSeen));
   }

   @Override
   public IracRequestStateCommand buildIracRequestStateCommand(IntSet segments) {
      return injectTracing(new IracRequestStateCommand(cacheName, segments));
   }

   @Override
   public IracStateResponseCommand buildIracStateResponseCommand(int capacity) {
      return injectTracing(new IracStateResponseCommand(cacheName, capacity));
   }

   @Override
   public IracPutKeyValueCommand buildIracPutKeyValueCommand(Object key, int segment, Object value, Metadata metadata,
         PrivateMetadata privateMetadata) {
      return injectTracing(new IracPutKeyValueCommand(key, segment, generateUUID(false), value, metadata, privateMetadata));
   }

   @Override
   public IracTouchKeyRequest buildIracTouchCommand(Object key) {
      return injectTracing(new IracTouchKeyRequest(cacheName, key));
   }

   @Override
   public IracUpdateVersionCommand buildIracUpdateVersionCommand(Map<Integer, IracEntryVersion> segmentsVersion) {
      return injectTracing(new IracUpdateVersionCommand(cacheName, segmentsVersion));
   }

   @Override
   public XSiteAutoTransferStatusCommand buildXSiteAutoTransferStatusCommand(String site) {
      return injectTracing(new XSiteAutoTransferStatusCommand(cacheName, site));
   }

   @Override
   public XSiteSetStateTransferModeCommand buildXSiteSetStateTransferModeCommand(String site, XSiteStateTransferMode mode) {
      return injectTracing(new XSiteSetStateTransferModeCommand(cacheName, site, mode));
   }

   @Override
   public IracTombstoneRemoteSiteCheckCommand buildIracTombstoneRemoteSiteCheckCommand(List<Object> keys) {
      return injectTracing(new IracTombstoneRemoteSiteCheckCommand(cacheName, keys));
   }

   @Override
   public IracTombstoneStateResponseCommand buildIracTombstoneStateResponseCommand(Collection<IracTombstoneInfo> state) {
      return injectTracing(new IracTombstoneStateResponseCommand(cacheName, state));
   }

   @Override
   public IracTombstonePrimaryCheckCommand buildIracTombstonePrimaryCheckCommand(Collection<IracTombstoneInfo> tombstones) {
      return injectTracing(new IracTombstonePrimaryCheckCommand(cacheName, tombstones));
   }

   @Override
   public IracPutManyRequest buildIracPutManyCommand(int capacity) {
      return injectTracing(new IracPutManyRequest(cacheName, capacity));
   }

   @Override
   public XSiteStatePushRequest buildXSiteStatePushRequest(XSiteState[] chunk, long timeoutMillis) {
      return injectTracing(new XSiteStatePushRequest(cacheName, chunk, timeoutMillis));
   }

   @Override
   public IracTombstoneCheckRequest buildIracTombstoneCheckRequest(List<Object> keys) {
      return injectTracing(new IracTombstoneCheckRequest(cacheName, keys));
   }

   private <T extends TracedCommand> T injectTracing(T command) {
      command.setTraceCommandData(new TracedCommand.TraceCommandData(cacheSpanAttribute, telemetry.createRemoteContext()));
      return command;
   }
}
