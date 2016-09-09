package org.infinispan.commands;

import static org.infinispan.xsite.XSiteAdminCommand.AdminOperation;
import static org.infinispan.xsite.statetransfer.XSiteStateTransferControlCommand.StateTransferControl;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.transaction.xa.Xid;

import org.infinispan.Cache;
import org.infinispan.atomic.Delta;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.functional.ReadOnlyKeyCommand;
import org.infinispan.commands.functional.ReadOnlyManyCommand;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyValueCommand;
import org.infinispan.commands.functional.ReadWriteManyCommand;
import org.infinispan.commands.functional.ReadWriteManyEntriesCommand;
import org.infinispan.commands.functional.WriteOnlyKeyCommand;
import org.infinispan.commands.functional.WriteOnlyKeyValueCommand;
import org.infinispan.commands.functional.WriteOnlyManyCommand;
import org.infinispan.commands.functional.WriteOnlyManyEntriesCommand;
import org.infinispan.commands.module.ModuleCommandInitializer;
import org.infinispan.commands.read.DistributedExecuteCommand;
import org.infinispan.commands.read.EntrySetCommand;
import org.infinispan.commands.read.GetAllCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.read.KeySetCommand;
import org.infinispan.commands.read.SizeCommand;
import org.infinispan.commands.remote.ClusteredGetAllCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.remote.GetKeysInGroupCommand;
import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.commands.remote.recovery.CompleteTransactionCommand;
import org.infinispan.commands.remote.recovery.GetInDoubtTransactionsCommand;
import org.infinispan.commands.remote.recovery.GetInDoubtTxInfoCommand;
import org.infinispan.commands.remote.recovery.TxCompletionNotificationCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.tx.VersionedCommitCommand;
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.commands.tx.totalorder.TotalOrderCommitCommand;
import org.infinispan.commands.tx.totalorder.TotalOrderNonVersionedPrepareCommand;
import org.infinispan.commands.tx.totalorder.TotalOrderRollbackCommand;
import org.infinispan.commands.tx.totalorder.TotalOrderVersionedCommitCommand;
import org.infinispan.commands.tx.totalorder.TotalOrderVersionedPrepareCommand;
import org.infinispan.commands.write.ApplyDeltaCommand;
import org.infinispan.commands.write.BackupAckCommand;
import org.infinispan.commands.write.BackupWriteCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.EvictCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.InvalidateL1Command;
import org.infinispan.commands.write.PrimaryAckCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.RemoveExpiredCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.ValueMatcher;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.api.functional.EntryView.ReadEntryView;
import org.infinispan.commons.api.functional.EntryView.ReadWriteEntryView;
import org.infinispan.commons.api.functional.EntryView.WriteEntryView;
import org.infinispan.commons.marshall.Externalizer;
import org.infinispan.commons.marshall.LambdaExternalizer;
import org.infinispan.commons.marshall.SerializeFunctionWith;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.group.GroupManager;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.functional.impl.Params;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.marshall.core.ExternalizerTable;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.partitionhandling.impl.PartitionHandlingManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.StateChunk;
import org.infinispan.statetransfer.StateConsumer;
import org.infinispan.statetransfer.StateProvider;
import org.infinispan.statetransfer.StateRequestCommand;
import org.infinispan.statetransfer.StateResponseCommand;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.stream.impl.ClusterStreamManager;
import org.infinispan.stream.impl.LocalStreamManager;
import org.infinispan.stream.impl.StreamRequestCommand;
import org.infinispan.stream.impl.StreamResponseCommand;
import org.infinispan.stream.impl.StreamSegmentResponseCommand;
import org.infinispan.transaction.impl.RemoteTransaction;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.xa.DldGlobalTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.recovery.RecoveryManager;
import org.infinispan.util.ByteString;
import org.infinispan.util.TimeService;
import org.infinispan.util.concurrent.CommandAckCollector;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.BackupSender;
import org.infinispan.xsite.SingleXSiteRpcCommand;
import org.infinispan.xsite.XSiteAdminCommand;
import org.infinispan.xsite.statetransfer.XSiteState;
import org.infinispan.xsite.statetransfer.XSiteStateConsumer;
import org.infinispan.xsite.statetransfer.XSiteStateProvider;
import org.infinispan.xsite.statetransfer.XSiteStatePushCommand;
import org.infinispan.xsite.statetransfer.XSiteStateTransferControlCommand;
import org.infinispan.xsite.statetransfer.XSiteStateTransferManager;

/**
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2011 Red Hat Inc.
 * @since 4.0
 */
public class CommandsFactoryImpl implements CommandsFactory {

   private static final Log log = LogFactory.getLog(CommandsFactoryImpl.class);
   private static final boolean trace = log.isTraceEnabled();


   private DataContainer dataContainer;
   private CacheNotifier<Object, Object> notifier;
   private Cache<Object, Object> cache;
   private ByteString cacheName;
   private boolean totalOrderProtocol;

   private InterceptorChain interceptorChain;
   private DistributionManager distributionManager;
   private InvocationContextFactory icf;
   private TransactionTable txTable;
   private Configuration configuration;
   private RecoveryManager recoveryManager;
   private StateProvider stateProvider;
   private StateConsumer stateConsumer;
   private LockManager lockManager;
   private InternalEntryFactory entryFactory;
   private StateTransferManager stateTransferManager;
   private BackupSender backupSender;
   private CancellationService cancellationService;
   private XSiteStateProvider xSiteStateProvider;
   private XSiteStateConsumer xSiteStateConsumer;
   private XSiteStateTransferManager xSiteStateTransferManager;
   private GroupManager groupManager;
   private LocalStreamManager localStreamManager;
   private ClusterStreamManager clusterStreamManager;
   private ClusteringDependentLogic clusteringDependentLogic;
   private TimeService timeService;
   private CommandAckCollector commandAckCollector;

   private Map<Byte, ModuleCommandInitializer> moduleCommandInitializers;
   private ExternalizerTable externalizerTable;

   @Inject
   public void setupDependencies(DataContainer container, CacheNotifier<Object, Object> notifier, Cache<Object, Object> cache,
                                 InterceptorChain interceptorChain, DistributionManager distributionManager,
                                 InvocationContextFactory icf, TransactionTable txTable, Configuration configuration,
                                 @ComponentName(KnownComponentNames.MODULE_COMMAND_INITIALIZERS) Map<Byte, ModuleCommandInitializer> moduleCommandInitializers,
                                 RecoveryManager recoveryManager, StateProvider stateProvider, StateConsumer stateConsumer,
                                 LockManager lockManager, InternalEntryFactory entryFactory,
                                 StateTransferManager stm, BackupSender backupSender, CancellationService cancellationService,
                                 TimeService timeService, XSiteStateProvider xSiteStateProvider, XSiteStateConsumer xSiteStateConsumer,
                                 XSiteStateTransferManager xSiteStateTransferManager,
                                 GroupManager groupManager, PartitionHandlingManager partitionHandlingManager,
                                 LocalStreamManager localStreamManager, ClusterStreamManager clusterStreamManager,
                                 ClusteringDependentLogic clusteringDependentLogic, ExternalizerTable externalizerTable,
                                 CommandAckCollector commandAckCollector) {
      this.dataContainer = container;
      this.notifier = notifier;
      this.cache = cache;
      this.interceptorChain = interceptorChain;
      this.distributionManager = distributionManager;
      this.icf = icf;
      this.txTable = txTable;
      this.configuration = configuration;
      this.moduleCommandInitializers = moduleCommandInitializers;
      this.recoveryManager = recoveryManager;
      this.stateProvider = stateProvider;
      this.stateConsumer = stateConsumer;
      this.lockManager = lockManager;
      this.entryFactory = entryFactory;
      this.stateTransferManager = stm;
      this.backupSender = backupSender;
      this.cancellationService = cancellationService;
      this.xSiteStateConsumer = xSiteStateConsumer;
      this.xSiteStateProvider = xSiteStateProvider;
      this.xSiteStateTransferManager = xSiteStateTransferManager;
      this.groupManager = groupManager;
      this.localStreamManager = localStreamManager;
      this.clusterStreamManager = clusterStreamManager;
      this.clusteringDependentLogic = clusteringDependentLogic;
      this.timeService = timeService;
      this.externalizerTable = externalizerTable;
      this.commandAckCollector = commandAckCollector;
   }

   @Start(priority = 1)
   // needs to happen early on
   public void start() {
      cacheName = ByteString.fromString(cache.getName());
      this.totalOrderProtocol = configuration.transaction().transactionProtocol().isTotalOrder();
   }

   @Override
   public PutKeyValueCommand buildPutKeyValueCommand(Object key, Object value, Metadata metadata, long flagsBitSet) {
      return new PutKeyValueCommand(key, value, false, notifier, metadata, flagsBitSet,
                                    configuration.dataContainer().valueEquivalence(), generateUUID());
   }

   @Override
   public RemoveCommand buildRemoveCommand(Object key, Object value, long flagsBitSet) {
      return new RemoveCommand(key, value, notifier, flagsBitSet, configuration.dataContainer().valueEquivalence(), generateUUID());
   }

   @Override
   public InvalidateCommand buildInvalidateCommand(long flagsBitSet, Object... keys) {
      return new InvalidateCommand(notifier, flagsBitSet, generateUUID(), keys);
   }

   @Override
   public InvalidateCommand buildInvalidateFromL1Command(long flagsBitSet, Collection<Object> keys) {
      return new InvalidateL1Command(dataContainer, distributionManager, notifier, flagsBitSet, keys, generateUUID());
   }

   @Override
   public InvalidateCommand buildInvalidateFromL1Command(Address origin, long flagsBitSet, Collection<Object> keys) {
      return new InvalidateL1Command(origin, dataContainer, distributionManager, notifier, flagsBitSet, keys, generateUUID());
   }

   @Override
   public RemoveExpiredCommand buildRemoveExpiredCommand(Object key, Object value, Long lifespan) {
      return new RemoveExpiredCommand(key, value, lifespan, notifier, configuration.dataContainer().valueEquivalence(),
              timeService, generateUUID());
   }

   @Override
   public ReplaceCommand buildReplaceCommand(Object key, Object oldValue, Object newValue, Metadata metadata, long flagsBitSet) {
      return new ReplaceCommand(key, oldValue, newValue, notifier, metadata, flagsBitSet, configuration.dataContainer().valueEquivalence(), generateUUID());
   }

   @Override
   public SizeCommand buildSizeCommand(Set<Flag> flags) {
      return new SizeCommand(cache, flags);
   }

   @Override
   public KeySetCommand buildKeySetCommand(Set<Flag> flags) {
      return new KeySetCommand(cache, flags);
   }

   @Override
   public EntrySetCommand buildEntrySetCommand(Set<Flag> flags) {
      return new EntrySetCommand(cache, flags);
   }

   @Override
   public GetKeyValueCommand buildGetKeyValueCommand(Object key, long flagsBitSet) {
      return new GetKeyValueCommand(key, flagsBitSet);
   }

   @Override
   public GetAllCommand buildGetAllCommand(Collection<?> keys, long flagsBitSet, boolean returnEntries) {
      return new GetAllCommand(keys, flagsBitSet, returnEntries, entryFactory);
   }

   @Override
   public PutMapCommand buildPutMapCommand(Map<?, ?> map, Metadata metadata, long flagsBitSet) {
      return new PutMapCommand(map, notifier, metadata, flagsBitSet, generateUUID());
   }

   @Override
   public ClearCommand buildClearCommand(long flagsBitSet) {
      return new ClearCommand(notifier, dataContainer, flagsBitSet);
   }

   @Override
   public EvictCommand buildEvictCommand(Object key, long flagsBitSet) {
      return new EvictCommand(key, notifier, flagsBitSet, generateUUID(), entryFactory);
   }

   @Override
   public PrepareCommand buildPrepareCommand(GlobalTransaction gtx, List<WriteCommand> modifications, boolean onePhaseCommit) {
      return totalOrderProtocol ? new TotalOrderNonVersionedPrepareCommand(cacheName, gtx, modifications) :
            new PrepareCommand(cacheName, gtx, modifications, onePhaseCommit);
   }

   @Override
   public VersionedPrepareCommand buildVersionedPrepareCommand(GlobalTransaction gtx, List<WriteCommand> modifications, boolean onePhase) {
      return totalOrderProtocol ? new TotalOrderVersionedPrepareCommand(cacheName, gtx, modifications, onePhase) :
            new VersionedPrepareCommand(cacheName, gtx, modifications, onePhase);
   }

   @Override
   public CommitCommand buildCommitCommand(GlobalTransaction gtx) {
      return totalOrderProtocol ? new TotalOrderCommitCommand(cacheName, gtx) :
            new CommitCommand(cacheName, gtx);
   }

   @Override
   public VersionedCommitCommand buildVersionedCommitCommand(GlobalTransaction gtx) {
      return totalOrderProtocol ? new TotalOrderVersionedCommitCommand(cacheName, gtx) :
            new VersionedCommitCommand(cacheName, gtx);
   }

   @Override
   public RollbackCommand buildRollbackCommand(GlobalTransaction gtx) {
      return totalOrderProtocol ? new TotalOrderRollbackCommand(cacheName, gtx) : new RollbackCommand(cacheName, gtx);
   }

   @Override
   public SingleRpcCommand buildSingleRpcCommand(ReplicableCommand call) {
      return new SingleRpcCommand(cacheName, call);
   }

   @Override
   public ClusteredGetCommand buildClusteredGetCommand(Object key, long flagsBitSet) {
      return new ClusteredGetCommand(key, cacheName, flagsBitSet, configuration.dataContainer().keyEquivalence());
   }

   /**
    * @param isRemote true if the command is deserialized and is executed remote.
    */
   @Override
   public void initializeReplicableCommand(ReplicableCommand c, boolean isRemote) {
      if (c == null) return;
      switch (c.getCommandId()) {
         case PutKeyValueCommand.COMMAND_ID:
            ((PutKeyValueCommand) c).init(notifier, configuration);
            break;
         case ReplaceCommand.COMMAND_ID:
            ((ReplaceCommand) c).init(notifier, configuration);
            break;
         case PutMapCommand.COMMAND_ID:
            ((PutMapCommand) c).init(notifier);
            break;
         case RemoveCommand.COMMAND_ID:
            ((RemoveCommand) c).init(notifier, configuration);
            break;
         case SingleRpcCommand.COMMAND_ID:
            SingleRpcCommand src = (SingleRpcCommand) c;
            src.init(interceptorChain, icf);
            if (src.getCommand() != null)
               initializeReplicableCommand(src.getCommand(), false);

            break;
         case InvalidateCommand.COMMAND_ID:
            InvalidateCommand ic = (InvalidateCommand) c;
            ic.init(notifier, configuration);
            break;
         case InvalidateL1Command.COMMAND_ID:
            InvalidateL1Command ilc = (InvalidateL1Command) c;
            ilc.init(configuration, distributionManager, notifier, dataContainer);
            break;
         case PrepareCommand.COMMAND_ID:
         case VersionedPrepareCommand.COMMAND_ID:
         case TotalOrderNonVersionedPrepareCommand.COMMAND_ID:
         case TotalOrderVersionedPrepareCommand.COMMAND_ID:
            PrepareCommand pc = (PrepareCommand) c;
            pc.init(interceptorChain, icf, txTable);
            pc.initialize(notifier, recoveryManager);
            if (pc.getModifications() != null)
               for (ReplicableCommand nested : pc.getModifications())  {
                  initializeReplicableCommand(nested, false);
               }
            pc.markTransactionAsRemote(isRemote);
            if (configuration.deadlockDetection().enabled() && isRemote) {
               DldGlobalTransaction transaction = (DldGlobalTransaction) pc.getGlobalTransaction();
               transaction.setLocksHeldAtOrigin(pc.getAffectedKeys());
            }
            break;
         case CommitCommand.COMMAND_ID:
         case VersionedCommitCommand.COMMAND_ID:
         case TotalOrderCommitCommand.COMMAND_ID:
         case TotalOrderVersionedCommitCommand.COMMAND_ID:
            CommitCommand commitCommand = (CommitCommand) c;
            commitCommand.init(interceptorChain, icf, txTable);
            commitCommand.markTransactionAsRemote(isRemote);
            break;
         case RollbackCommand.COMMAND_ID:
         case TotalOrderRollbackCommand.COMMAND_ID:
            RollbackCommand rollbackCommand = (RollbackCommand) c;
            rollbackCommand.init(interceptorChain, icf, txTable);
            rollbackCommand.markTransactionAsRemote(isRemote);
            break;
         case ClearCommand.COMMAND_ID:
            ClearCommand cc = (ClearCommand) c;
            cc.init(notifier, dataContainer);
            break;
         case ClusteredGetCommand.COMMAND_ID:
            ClusteredGetCommand clusteredGetCommand = (ClusteredGetCommand) c;
            clusteredGetCommand.initialize(icf, this, entryFactory,
                                           interceptorChain,
               configuration.dataContainer().keyEquivalence());
            break;
         case LockControlCommand.COMMAND_ID:
            LockControlCommand lcc = (LockControlCommand) c;
            lcc.init(interceptorChain, icf, txTable);
            lcc.markTransactionAsRemote(isRemote);
            if (configuration.deadlockDetection().enabled() && isRemote) {
               DldGlobalTransaction gtx = (DldGlobalTransaction) lcc.getGlobalTransaction();
               RemoteTransaction transaction = txTable.getRemoteTransaction(gtx);
               if (transaction != null) {
                  if (!configuration.clustering().cacheMode().isDistributed()) {
                     Set<Object> keys = txTable.getLockedKeysForRemoteTransaction(gtx);
                     GlobalTransaction gtx2 = transaction.getGlobalTransaction();
                     ((DldGlobalTransaction) gtx2).setLocksHeldAtOrigin(keys);
                     gtx.setLocksHeldAtOrigin(keys);
                  } else {
                     GlobalTransaction gtx2 = transaction.getGlobalTransaction();
                     ((DldGlobalTransaction) gtx2).setLocksHeldAtOrigin(gtx.getLocksHeldAtOrigin());
                  }
               }
            }
            break;
         case StateRequestCommand.COMMAND_ID:
            ((StateRequestCommand) c).init(stateProvider);
            break;
         case StateResponseCommand.COMMAND_ID:
            ((StateResponseCommand) c).init(stateConsumer);
            break;
         case GetInDoubtTransactionsCommand.COMMAND_ID:
            GetInDoubtTransactionsCommand gptx = (GetInDoubtTransactionsCommand) c;
            gptx.init(recoveryManager);
            break;
         case TxCompletionNotificationCommand.COMMAND_ID:
            TxCompletionNotificationCommand ftx = (TxCompletionNotificationCommand) c;
            ftx.init(txTable, lockManager, recoveryManager, stateTransferManager);
            break;
         case DistributedExecuteCommand.COMMAND_ID:
            DistributedExecuteCommand dec = (DistributedExecuteCommand)c;
            dec.init(cache);
            break;
         case GetInDoubtTxInfoCommand.COMMAND_ID:
            GetInDoubtTxInfoCommand gidTxInfoCommand = (GetInDoubtTxInfoCommand)c;
            gidTxInfoCommand.init(recoveryManager);
            break;
         case CompleteTransactionCommand.COMMAND_ID:
            CompleteTransactionCommand ccc = (CompleteTransactionCommand)c;
            ccc.init(recoveryManager);
            break;
         case ApplyDeltaCommand.COMMAND_ID:
            break;
         case CreateCacheCommand.COMMAND_ID:
            CreateCacheCommand createCacheCommand = (CreateCacheCommand)c;
            createCacheCommand.init(cache.getCacheManager());
            break;
         case XSiteAdminCommand.COMMAND_ID:
            XSiteAdminCommand xSiteAdminCommand = (XSiteAdminCommand)c;
            xSiteAdminCommand.init(backupSender);
            break;
         case CancelCommand.COMMAND_ID:
            CancelCommand cancelCommand = (CancelCommand)c;
            cancelCommand.init(cancellationService);
            break;
         case XSiteStateTransferControlCommand.COMMAND_ID:
            XSiteStateTransferControlCommand xSiteStateTransferControlCommand = (XSiteStateTransferControlCommand) c;
            xSiteStateTransferControlCommand.initialize(xSiteStateProvider, xSiteStateConsumer, xSiteStateTransferManager);
            break;
         case XSiteStatePushCommand.COMMAND_ID:
            XSiteStatePushCommand xSiteStatePushCommand = (XSiteStatePushCommand) c;
            xSiteStatePushCommand.initialize(xSiteStateConsumer);
            break;
         case GetKeysInGroupCommand.COMMAND_ID:
            GetKeysInGroupCommand getKeysInGroupCommand = (GetKeysInGroupCommand) c;
            getKeysInGroupCommand.setGroupManager(groupManager);
            break;
         case ClusteredGetAllCommand.COMMAND_ID:
            ClusteredGetAllCommand clusteredGetAllCommand = (ClusteredGetAllCommand) c;
            clusteredGetAllCommand.init(icf, this, entryFactory, interceptorChain, txTable,
                  configuration.dataContainer().keyEquivalence());
            break;
         case StreamRequestCommand.COMMAND_ID:
            StreamRequestCommand streamRequestCommand = (StreamRequestCommand) c;
            streamRequestCommand.inject(localStreamManager);
            break;
         case StreamResponseCommand.COMMAND_ID:
            StreamResponseCommand streamResponseCommand = (StreamResponseCommand) c;
            streamResponseCommand.inject(clusterStreamManager);
            break;
         case StreamSegmentResponseCommand.COMMAND_ID:
            StreamSegmentResponseCommand streamSegmentResponseCommand = (StreamSegmentResponseCommand) c;
            streamSegmentResponseCommand.inject(clusterStreamManager);
            break;
         case RemoveExpiredCommand.COMMAND_ID:
            RemoveExpiredCommand removeExpiredCommand = (RemoveExpiredCommand) c;
            removeExpiredCommand.init(notifier, configuration);
            break;
         case BackupAckCommand.COMMAND_ID:
            BackupAckCommand command = (BackupAckCommand) c;
            command.setCommandAckCollector(commandAckCollector);
            break;
         case BackupWriteCommand.COMMAND_ID:
            BackupWriteCommand bwc = (BackupWriteCommand) c;
            bwc.setNotifier(notifier);
            break;
         case PrimaryAckCommand.COMMAND_ID:
            PrimaryAckCommand pac = (PrimaryAckCommand) c;
            pac.setCommandAckCollector(commandAckCollector);
            break;
         default:
            ModuleCommandInitializer mci = moduleCommandInitializers.get(c.getCommandId());
            if (mci != null) {
               mci.initializeReplicableCommand(c, isRemote);
            } else {
               if (trace) log.tracef("Nothing to initialize for command: %s", c);
            }
      }
   }

   @Override
   public LockControlCommand buildLockControlCommand(Collection<?> keys, long flagsBitSet, GlobalTransaction gtx) {
      return new LockControlCommand(keys, cacheName, flagsBitSet, gtx);
   }

   @Override
   public LockControlCommand buildLockControlCommand(Object key, long flagsBitSet, GlobalTransaction gtx) {
      return new LockControlCommand(key, cacheName, flagsBitSet, gtx);
   }

   @Override
   public LockControlCommand buildLockControlCommand(Collection<?> keys, long flagsBitSet) {
      return new LockControlCommand(keys, cacheName, flagsBitSet, null);
   }

   @Override
   public StateRequestCommand buildStateRequestCommand(StateRequestCommand.Type subtype, Address sender, int viewId, Set<Integer> segments) {
      return new StateRequestCommand(cacheName, subtype, sender, viewId, segments);
   }

   @Override
   public StateResponseCommand buildStateResponseCommand(Address sender, int topologyId, Collection<StateChunk> stateChunks) {
      return new StateResponseCommand(cacheName, sender, topologyId, stateChunks);
   }

   @Override
   public String getCacheName() {
      return cacheName.toString();
   }

   @Override
   public GetInDoubtTransactionsCommand buildGetInDoubtTransactionsCommand() {
      return new GetInDoubtTransactionsCommand(cacheName);
   }

   @Override
   public TxCompletionNotificationCommand buildTxCompletionNotificationCommand(Xid xid, GlobalTransaction globalTransaction) {
      return new TxCompletionNotificationCommand(xid, globalTransaction, cacheName);
   }

   @Override
   public TxCompletionNotificationCommand buildTxCompletionNotificationCommand(long internalId) {
      return new TxCompletionNotificationCommand(internalId, cacheName);
   }

   @Override
   public <T> DistributedExecuteCommand<T> buildDistributedExecuteCommand(Callable<T> callable, Address sender, Collection keys) {
      return new DistributedExecuteCommand<T>(cacheName, keys, callable);
   }

   @Override
   public GetInDoubtTxInfoCommand buildGetInDoubtTxInfoCommand() {
      return new GetInDoubtTxInfoCommand(cacheName);
   }

   @Override
   public CompleteTransactionCommand buildCompleteTransactionCommand(Xid xid, boolean commit) {
      return new CompleteTransactionCommand(cacheName, xid, commit);
   }

   @Override
   public ApplyDeltaCommand buildApplyDeltaCommand(Object deltaAwareValueKey, Delta delta, Collection keys) {
      return new ApplyDeltaCommand(deltaAwareValueKey, delta, keys, generateUUID());
   }

   @Override
   public CreateCacheCommand buildCreateCacheCommand(String cacheNameToCreate, String cacheConfigurationName) {
      return new CreateCacheCommand(cacheName, cacheNameToCreate, cacheConfigurationName);
   }

   @Override
   public CreateCacheCommand buildCreateCacheCommand(String cacheNameToCreate, String cacheConfigurationName, int size) {
      return new CreateCacheCommand(cacheName, cacheNameToCreate, cacheConfigurationName, size);
   }

   @Override
   public CancelCommand buildCancelCommandCommand(UUID commandUUID) {
      return new CancelCommand(cacheName, commandUUID);
   }

   @Override
   public XSiteStateTransferControlCommand buildXSiteStateTransferControlCommand(StateTransferControl control,
                                                                                 String siteName) {
      return new XSiteStateTransferControlCommand(cacheName, control, siteName);
   }

   @Override
   public XSiteAdminCommand buildXSiteAdminCommand(String siteName, AdminOperation op, Integer afterFailures,
                                                   Long minTimeToWait) {
      return new XSiteAdminCommand(cacheName, siteName, op, afterFailures, minTimeToWait);
   }

   @Override
   public XSiteStatePushCommand buildXSiteStatePushCommand(XSiteState[] chunk, long timeoutMillis) {
      return new XSiteStatePushCommand(cacheName, chunk, timeoutMillis);
   }

   @Override
   public SingleXSiteRpcCommand buildSingleXSiteRpcCommand(VisitableCommand command) {
      return new SingleXSiteRpcCommand(cacheName, command);
   }

   @Override
   public GetKeysInGroupCommand buildGetKeysInGroupCommand(long flagsBitSet, String groupName) {
      return new GetKeysInGroupCommand(flagsBitSet, groupName).setGroupManager(groupManager);
   }

   @Override
   public <K> StreamRequestCommand<K> buildStreamRequestCommand(Object id, boolean parallelStream,
           StreamRequestCommand.Type type, Set<Integer> segments, Set<K> keys, Set<K> excludedKeys,
           boolean includeLoader, Object terminalOperation) {
      return new StreamRequestCommand<>(cacheName, cache.getCacheManager().getAddress(), id, parallelStream, type,
              segments, keys, excludedKeys, includeLoader, terminalOperation);
   }

   @Override
   public <R> StreamResponseCommand<R> buildStreamResponseCommand(Object identifier, boolean complete,
           Set<Integer> lostSegments, R response) {
      if (lostSegments.isEmpty()) {
         return new StreamResponseCommand<>(cacheName, cache.getCacheManager().getAddress(), identifier, complete,
                 response);
      } else {
         return new StreamSegmentResponseCommand<>(cacheName, cache.getCacheManager().getAddress(), identifier,
                 complete, response, lostSegments);
      }
   }

   @Override
   public GetCacheEntryCommand buildGetCacheEntryCommand(Object key, long flagsBitSet) {
      return new GetCacheEntryCommand(key, flagsBitSet, entryFactory);
   }

   @Override
   public ClusteredGetAllCommand buildClusteredGetAllCommand(List<?> keys, long flagsBitSet, GlobalTransaction gtx) {
      return new ClusteredGetAllCommand(cacheName, keys, flagsBitSet, gtx, configuration.dataContainer().keyEquivalence());
   }

   private CommandInvocationId generateUUID() {
      return CommandInvocationId.generateId(clusteringDependentLogic.getAddress());
   }

   @Override
   public <K, V, R> ReadOnlyKeyCommand<K, V, R> buildReadOnlyKeyCommand(K key, Function<ReadEntryView<K, V>, R> f) {
      return new ReadOnlyKeyCommand<>(key, f);
   }

   @Override
   public <K, V, R> ReadOnlyManyCommand<K, V, R> buildReadOnlyManyCommand(Collection<? extends K> keys, Function<ReadEntryView<K, V>, R> f) {
      return new ReadOnlyManyCommand<>(keys, f);
   }

   @Override
   public <K, V, R> ReadWriteKeyValueCommand<K, V, R> buildReadWriteKeyValueCommand(
         K key, V value, BiFunction<V, ReadWriteEntryView<K, V>, R> f, Params params) {
      return new ReadWriteKeyValueCommand<>(key, value, f, generateUUID(), getValueMatcher(f), params);
   }

   @Override
   public <K, V, R> ReadWriteKeyCommand<K, V, R> buildReadWriteKeyCommand(
         K key, Function<ReadWriteEntryView<K, V>, R> f, Params params) {
      return new ReadWriteKeyCommand<>(key, f, generateUUID(), getValueMatcher(f), params);
   }

   @Override
   public <K, V, R> ReadWriteManyCommand<K, V, R> buildReadWriteManyCommand(Collection<? extends K> keys, Function<ReadWriteEntryView<K, V>, R> f, Params params) {
      return new ReadWriteManyCommand<>(keys, f, params);
   }

   @Override
   public <K, V, R> ReadWriteManyEntriesCommand<K, V, R> buildReadWriteManyEntriesCommand(Map<? extends K, ? extends V> entries, BiFunction<V, ReadWriteEntryView<K, V>, R> f, Params params) {
      return new ReadWriteManyEntriesCommand<>(entries, f, params);
   }

   @Override
   public <K, V> WriteOnlyKeyCommand<K, V> buildWriteOnlyKeyCommand(
         K key, Consumer<WriteEntryView<V>> f, Params params) {
      return new WriteOnlyKeyCommand<>(key, f, generateUUID(), getValueMatcher(f), params);
   }

   @Override
   public <K, V> WriteOnlyKeyValueCommand<K, V> buildWriteOnlyKeyValueCommand(
         K key, V value, BiConsumer<V, WriteEntryView<V>> f, Params params) {
      return new WriteOnlyKeyValueCommand<>(key, value, f, generateUUID(), getValueMatcher(f), params);
   }

   @Override
   public <K, V> WriteOnlyManyCommand<K, V> buildWriteOnlyManyCommand(Collection<? extends K> keys, Consumer<WriteEntryView<V>> f, Params params) {
      return new WriteOnlyManyCommand<>(keys, f, params);
   }

   @Override
   public <K, V> WriteOnlyManyEntriesCommand<K, V> buildWriteOnlyManyEntriesCommand(
         Map<? extends K, ? extends V> entries, BiConsumer<V, WriteEntryView<V>> f, Params params) {
      return new WriteOnlyManyEntriesCommand<>(entries, f, params);
   }

   @Override
   public BackupAckCommand buildBackupAckCommand(CommandInvocationId id) {
      BackupAckCommand cmd = new BackupAckCommand(cacheName);
      cmd.setCommandInvocationId(id);
      return cmd;
   }

   @Override
   public PrimaryAckCommand buildPrimaryAckCommand(CommandInvocationId id, Object returnValue, boolean success) {
      PrimaryAckCommand cmd = new PrimaryAckCommand(cacheName);
      cmd.setCommandInvocationId(id);
      cmd.setReturnValue(returnValue);
      cmd.setSuccess(success);
      return cmd;
   }

   private ValueMatcher getValueMatcher(Object o) {
      SerializeFunctionWith ann = o.getClass().getAnnotation(SerializeFunctionWith.class);
      if (ann != null)
         return ValueMatcher.valueOf(ann.valueMatcher().toString());

      Externalizer ext = externalizerTable.getExternalizer(o);
      if (ext != null && ext instanceof LambdaExternalizer)
         return ValueMatcher.valueOf(((LambdaExternalizer) ext).valueMatcher(o).toString());

      return ValueMatcher.MATCH_ALWAYS;
   }
}
