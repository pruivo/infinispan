/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.infinispan.statetransfer;

import org.infinispan.Cache;
import org.infinispan.CacheException;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.control.ShadowTransactionCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.container.versioning.gmu.GMUCacheEntryVersion;
import org.infinispan.container.versioning.gmu.GMUVersionGenerator;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.interceptors.gmu.GMUEntryWrappingInterceptor;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.CacheStore;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.reconfigurableprotocol.manager.ProtocolManager;
import org.infinispan.reconfigurableprotocol.manager.ReconfigurableReplicationManager;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.CacheTopology;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.RemoteTransaction;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.gmu.manager.SortedTransactionQueue;
import org.infinispan.transaction.gmu.manager.TransactionCommitManager;
import org.infinispan.transaction.totalorder.TotalOrderLatch;
import org.infinispan.transaction.totalorder.TotalOrderManager;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.util.InfinispanCollections;
import org.infinispan.util.ReadOnlyDataContainerBackedKeySet;
import org.infinispan.util.concurrent.*;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.infinispan.context.Flag.*;
import static org.infinispan.factories.KnownComponentNames.ASYNC_TRANSPORT_EXECUTOR;

/**
 * {@link StateConsumer} implementation.
 *
 * @author anistor@redhat.com
 * @author Sebastiano Peluso
 * @since 5.2
 */
public class StateConsumerImpl implements StateConsumer {

   protected static final Log log = LogFactory.getLog(StateConsumerImpl.class);
   private static final boolean trace = log.isTraceEnabled();

   private ExecutorService executorService;
   private StateTransferManager stateTransferManager;
   protected String cacheName;
   private Configuration configuration;
   protected RpcManager rpcManager;
   private TransactionManager transactionManager;   // optional
   protected CommandsFactory commandsFactory;
   private TransactionTable transactionTable;       // optional
   private DataContainer dataContainer;
   private CacheLoaderManager cacheLoaderManager;
   private InterceptorChain interceptorChain;
   private InvocationContextContainer icc;
   private StateTransferLock stateTransferLock;
   private CacheNotifier cacheNotifier;
   private TotalOrderManager totalOrderManager;
   private ProtocolManager protocolManager;
   protected long timeout;
   private boolean useVersionedPut;
   protected boolean isFetchEnabled;
   protected boolean isTransactional;
   private boolean isInvalidationMode;

   private BlockingTaskAwareExecutorService gmuExecutorService;

   protected volatile CacheTopology cacheTopology;

   /**
    * Keeps track of all keys updated by user code during state transfer. If this is null no keys are being recorded and
    * state transfer is not allowed to update anything. This can be null if there is not state transfer in progress at
    * the moment of there is one but a ClearCommand was encountered.
    */
   private volatile Set<Object> updatedKeys;

   /**
    * Indicates if there is a rebalance in progress. It is set to true when onTopologyUpdate with isRebalance==true is called.
    * It becomes false when a topology update with a null pending CH is received.
    */
   private final AtomicBoolean rebalanceInProgress = new AtomicBoolean(false);

   /**
    * Indicates if there is a rebalance in progress and there the local node has not yet received
    * all the new segments yet. It is set to true when rebalance starts and becomes when all inbound transfers have completed
    * (before rebalanceInProgress is set back to false).
    */
   private final AtomicBoolean waitingForState = new AtomicBoolean(false);

   /**
    * A map that keeps track of current inbound state transfers by source address. There could be multiple transfers
    * flowing in from the same source (but for different segments) so the values are lists. This works in tandem with
    * transfersBySegment so they always need to be kept in sync and updates to both of them need to be atomic.
    */
   protected final Map<Address, List<InboundTransferTask>> transfersBySource = new HashMap<Address, List<InboundTransferTask>>();

   /**
    * A map that keeps track of current inbound state transfers by segment id. There is at most one transfers per segment.
    * This works in tandem with transfersBySource so they always need to be kept in sync and updates to both of them
    * need to be atomic.
    */
   private final Map<Integer, InboundTransferTask> transfersBySegment = new HashMap<Integer, InboundTransferTask>();

   /**
    * Tasks ready to be executed by the transfer thread. These tasks are also present if transfersBySegment and transfersBySource.
    */
   protected final BlockingDeque<InboundTransferTask> taskQueue = new LinkedBlockingDeque<InboundTransferTask>();

   private boolean isTransferThreadRunning;

   private volatile boolean ownsData = false;
   private ConcurrentMap<Object, OwnersList> oldKeyOwners;
   private VersionGenerator versionGenerator;
   private final Map<Address, TransactionInfo> shadowTransactionInfoReceiverMap = new HashMap<Address, TransactionInfo>();
   private TransactionCommitManager transactionCommitManager;

   public StateConsumerImpl() {
   }

   /**
    * Stops applying incoming state. Also stops tracking updated keys. Should be called at the end of state transfer or
    * when a ClearCommand is committed during state transfer.
    */
   @Override
   public void stopApplyingState() {
      if (trace) log.tracef("Stop keeping track of changed keys for state transfer");
      updatedKeys = null;
   }

   /**
    * Receive notification of updated keys right before they are committed in DataContainer.
    *
    * @param key the key that is being modified
    */
   @Override
   public void addUpdatedKey(Object key) {
      // grab a copy of the reference to prevent issues if another thread calls stopApplyingState() between null check and actual usage
      final Set<Object> localUpdatedKeys = updatedKeys;
      if (localUpdatedKeys != null) {
         if (cacheTopology.getWriteConsistentHash().isKeyLocalToNode(rpcManager.getAddress(), key)) {
            if (log.isTraceEnabled()) {
               log.tracef("Mark key [%s] as updated", key);
            }
            localUpdatedKeys.add(key);
         }
      }
   }

   /**
    * Checks if a given key was updated by user code during state transfer (and consequently it is untouchable by state transfer).
    *
    * @param key the key to check
    * @return true if the key is known to be modified, false otherwise
    */
   @Override
   public boolean isKeyUpdated(Object key) {
      // grab a copy of the reference to prevent issues if another thread calls stopApplyingState() between null check and actual usage
      final Set<Object> localUpdatedKeys = updatedKeys;
      boolean updated = localUpdatedKeys == null || localUpdatedKeys.contains(key);
      if (log.isTraceEnabled()) {
         log.tracef("Is key [%s] updated? %s (collection is null? %s)", key, updated, localUpdatedKeys == null);
      }
      return updated;
   }

   @Inject
   public void init(Cache cache,
                    @ComponentName(KnownComponentNames.TOPOLOGY_EXECUTOR) BlockingTaskAwareExecutorService executorService,
                    StateTransferManager stateTransferManager,
                    InterceptorChain interceptorChain,
                    InvocationContextContainer icc,
                    Configuration configuration,
                    RpcManager rpcManager,
                    TransactionManager transactionManager,
                    CommandsFactory commandsFactory,
                    CacheLoaderManager cacheLoaderManager,
                    DataContainer dataContainer,
                    TransactionTable transactionTable,
                    StateTransferLock stateTransferLock,
                    CacheNotifier cacheNotifier,
                    TotalOrderManager totalOrderManager,
                    VersionGenerator versionGenerator,
                    @ComponentName(KnownComponentNames.GMU_EXECUTOR) BlockingTaskAwareExecutorService gmuExecutorService,
                    ReconfigurableReplicationManager reconfigurableReplicationManager,
                    TransactionCommitManager transactionCommitManager) {
      this.cacheName = cache.getName();
      this.executorService = executorService;
      this.stateTransferManager = stateTransferManager;
      this.interceptorChain = interceptorChain;
      this.icc = icc;
      this.configuration = configuration;
      this.rpcManager = rpcManager;
      this.transactionManager = transactionManager;
      this.commandsFactory = commandsFactory;
      this.cacheLoaderManager = cacheLoaderManager;
      this.dataContainer = dataContainer;
      this.transactionTable = transactionTable;
      this.stateTransferLock = stateTransferLock;
      this.cacheNotifier = cacheNotifier;
      this.totalOrderManager = totalOrderManager;
      this.versionGenerator = versionGenerator;
      this.gmuExecutorService = gmuExecutorService;
      this.protocolManager = reconfigurableReplicationManager.getProtocolManager();
      this.transactionCommitManager = transactionCommitManager;

      isInvalidationMode = configuration.clustering().cacheMode().isInvalidation();

      isTransactional = configuration.transaction().transactionMode().isTransactional();

      // we need to use a special form of PutKeyValueCommand that can apply versions too
      useVersionedPut = isTransactional &&
            Configurations.isVersioningEnabled(configuration) &&
            configuration.clustering().cacheMode().isClustered();

      timeout = configuration.clustering().stateTransfer().timeout();
      if (configuration.locking().isolationLevel() == IsolationLevel.SERIALIZABLE) {
         oldKeyOwners = ConcurrentMapFactory.makeConcurrentMap();
      }
   }

   public boolean hasActiveTransfers() {
      synchronized (this) {
         return !transfersBySource.isEmpty();
      }
   }

   @Override
   public boolean isStateTransferInProgress() {
      return rebalanceInProgress.get();
   }

   @Override
   public boolean isStateTransferInProgressForKey(Object key) {
      if (isInvalidationMode) {
         // In invalidation mode it is of not much relevance if the key is actually being transferred right now.
         // A false response to this will just mean the usual remote lookup before a write operation is not
         // performed and a null is assumed. But in invalidation mode the user must expect the data can disappear
         // from cache at any time so this null previous value should not cause any trouble.
         return false;
      }
      synchronized (this) {
         CacheTopology localCacheTopology = cacheTopology;
         if (localCacheTopology == null || localCacheTopology.getPendingCH() == null)
            return false;
         Address address = rpcManager.getAddress();
         boolean keyWillBeLocal = localCacheTopology.getPendingCH().isKeyLocalToNode(address, key);
         boolean keyIsLocal = localCacheTopology.getCurrentCH().isKeyLocalToNode(address, key);
         return keyWillBeLocal && !keyIsLocal;
      }
   }

   @Override
   public boolean ownsData() {
      return ownsData;
   }

   @Override
   public List<Address> oldOwners(Object key) {
      if (oldKeyOwners == null) {
         return InfinispanCollections.emptyList();
      }
      OwnersList list = oldKeyOwners.get(key);
      return list == null ? InfinispanCollections.<Address>emptyList() : list.toList();
   }

   @Override
   public void onTopologyUpdate(final CacheTopology cacheTopology, final boolean isRebalance) {
      final boolean isMember = cacheTopology.getMembers().contains(rpcManager.getAddress());
      if (trace)
         log.tracef("Received new topology for cache %s, isRebalance = %b, isMember = %b, topology = %s", cacheName, isRebalance, isMember, cacheTopology);

      if (isRebalance) {
         if (!ownsData && cacheTopology.getMembers().contains(rpcManager.getAddress())) {
            ownsData = true;
         }
         rebalanceInProgress.set(true);
         cacheNotifier.notifyDataRehashed(cacheTopology.getCurrentCH(), cacheTopology.getPendingCH(),
                                          cacheTopology.getTopologyId(), true);

         //in total order, we should wait for remote transactions before proceeding
         if (isTotalOrder()) {
            if (log.isTraceEnabled()) {
               log.trace("State Transfer in Total Order cache. Waiting for remote transactions to finish");
            }
            try {
               for (TotalOrderLatch block : totalOrderManager.notifyStateTransferStart(cacheTopology.getTopologyId())) {
                  block.awaitUntilUnBlock();
               }
            } catch (InterruptedException e) {
               //interrupted...
               Thread.currentThread().interrupt();
               throw new CacheException(e);
            }
            if (log.isTraceEnabled()) {
               log.trace("State Transfer in Total Order cache. All remote transactions are finished. Moving on...");
            }
         }

         if (log.isTraceEnabled()) {
            log.tracef("Lock State Transfer in Progress for topology ID %s", cacheTopology.getTopologyId());
         }
      } else {
         if (cacheTopology.getMembers().size() == 1 && cacheTopology.getMembers().get(0).equals(rpcManager.getAddress())) {
            //we are the first member in the cache...
            ownsData = true;
         }
      }

      // Make sure we don't send a REBALANCE_CONFIRM command before we've added all the transfer tasks
      // even if some of the tasks are removed and re-added
      waitingForState.set(false);

      final ConsistentHash previousReadCh = this.cacheTopology != null ? this.cacheTopology.getReadConsistentHash() : null;
      final ConsistentHash previousWriteCh = this.cacheTopology != null ? this.cacheTopology.getWriteConsistentHash() : null;
      // Ensures writes to the data container use the right consistent hash
      // No need for a try/finally block, since it's just an assignment
      stateTransferLock.acquireExclusiveTopologyLock();
      this.cacheTopology = cacheTopology;
      if (isRebalance) {
         if (trace) log.tracef("Start keeping track of keys for rebalance");
         updatedKeys = new ConcurrentHashSet<Object>();
      }
      stateTransferLock.releaseExclusiveTopologyLock();
      stateTransferLock.notifyTopologyInstalled(cacheTopology.getTopologyId());

      try {
         // fetch transactions and data segments from other owners if this is enabled
         if (isTransactional || isFetchEnabled) {
            Set<Integer> addedSegments;
            if (previousWriteCh == null) {
               // we start fresh, without any data, so we need to pull everything we own according to writeCh

               addedSegments = getOwnedSegments(cacheTopology.getWriteConsistentHash());

               if (trace) {
                  log.tracef("On cache %s we have: added segments: %s", cacheName, addedSegments);
               }
            } else {
               Set<Integer> previousSegments = getOwnedSegments(previousWriteCh);
               Set<Integer> newSegments = getOwnedSegments(cacheTopology.getWriteConsistentHash());

               Set<Integer> removedSegments = new HashSet<Integer>(previousSegments);
               removedSegments.removeAll(newSegments);

               // This is a rebalance, we need to request the segments we own in the new CH.
               addedSegments = new HashSet<Integer>(newSegments);
               addedSegments.removeAll(previousSegments);

               if (trace) {
                  log.tracef("On cache %s we have: removed segments: %s; new segments: %s; old segments: %s; added segments: %s",
                             cacheName, removedSegments, newSegments, previousSegments, addedSegments);
               }

               // remove inbound transfers for segments we no longer own
               cancelTransfers(removedSegments);

               // if we are still a member then we need to discard/move to L1 the segments we no longer own
               if (isMember) {
                  // any data for segments we no longer own should be removed from data container and cache store or moved to L1 if enabled
                  // If L1.onRehash is enabled, "removed" segments are actually moved to L1. The new (and old) owners
                  // will automatically add the nodes that no longer own a key to that key's requestors list.
                  invalidateSegments(cacheTopology.getWriteConsistentHash(), removedSegments);
               }

               // check if any of the existing transfers should be restarted from a different source because the initial source is no longer a member
               restartBrokenTransfers(cacheTopology, addedSegments);
            }

            if (!addedSegments.isEmpty()) {
               addTransfers(addedSegments);  // add transfers for new or restarted segments
            }
            afterStateTransferStarted(previousWriteCh, cacheTopology.getWriteConsistentHash());
         }

         log.tracef("Topology update processed, rebalanceInProgress = %s, isRebalance = %s, pending CH = %s",
                    rebalanceInProgress.get(), isRebalance, cacheTopology.getPendingCH());
         if (rebalanceInProgress.get()) {
            // there was a rebalance in progress
            if (!isRebalance && cacheTopology.getPendingCH() == null) {
               // we have received a topology update without a pending CH, signalling the end of the rebalance
               boolean changed = rebalanceInProgress.compareAndSet(true, false);
               if (changed) {
                  // if the coordinator changed, we might get two concurrent topology updates,
                  // but we only want to notify the @DataRehashed listeners once
                  cacheNotifier.notifyDataRehashed(previousReadCh, cacheTopology.getCurrentCH(),
                                                   cacheTopology.getTopologyId(), false);
                  if (log.isTraceEnabled()) {
                     log.tracef("Unlock State Transfer in Progress for topology ID %s", cacheTopology.getTopologyId());
                  }
                  if (isTotalOrder()) {
                     totalOrderManager.notifyStateTransferEnd();
                  }
               }
            }
         }
      } finally {
         stateTransferLock.notifyTransactionDataReceived(cacheTopology.getTopologyId());

         // Only set the flag here, after all the transfers have been added to the transfersBySource map
         if (rebalanceInProgress.get()) {
            waitingForState.set(true);
         }

         notifyEndOfRebalanceIfNeeded(cacheTopology.getTopologyId());

         // Remove the transactions whose originators have left the cache.
         // Need to do it now, after we have applied any transactions from other nodes,
         // and after notifyTransactionDataReceived - otherwise the RollbackCommands would block.
         if (transactionTable != null) {
            transactionTable.cleanupStaleTransactions(cacheTopology);
         }
      }
   }

   protected void afterStateTransferStarted(ConsistentHash oldCh, ConsistentHash newCh) {
      //no-op
   }

   private void notifyEndOfRebalanceIfNeeded(int topologyId) {
      if (waitingForState.get() && !hasActiveTransfers()) {
         if (waitingForState.compareAndSet(true, false)) {
            log.debugf("Finished receiving of segments for cache %s for topology %d.", cacheName, topologyId);
            stopApplyingState();
            stateTransferManager.notifyEndOfRebalance(topologyId);
         }
      }
   }

   private Set<Integer> getOwnedSegments(ConsistentHash consistentHash) {
      Address address = rpcManager.getAddress();
      return consistentHash.getMembers().contains(address) ? consistentHash.getSegmentsForOwner(address)
            : InfinispanCollections.<Integer>emptySet();
   }

   public void applyState(Address sender, int topologyId, Collection<StateChunk> stateChunks, EntryVersion txVersion) {
      ConsistentHash wCh = cacheTopology.getWriteConsistentHash();
      // ignore responses received after we are no longer a member
      if (!wCh.getMembers().contains(rpcManager.getAddress())) {
         if (trace) {
            log.tracef("Ignoring received state because we are no longer a member");
         }

         return;
      }

      if (trace) {
         log.tracef("Before applying the received state the data container of cache %s has %d keys", cacheName, dataContainer.size(null));
      }

      for (StateChunk stateChunk : stateChunks) {
         // it's possible to receive a late message so we must be prepared to ignore segments we no longer own
         //todo [anistor] this check should be based on topologyId
         if (stateChunk.getSegmentId() != -1 && !wCh.getSegmentsForOwner(rpcManager.getAddress()).contains(stateChunk.getSegmentId())) {
            log.warnf("Discarding received cache entries for segment %d of cache %s because they do not belong to this node.", stateChunk.getSegmentId(), cacheName);
            continue;
         }

         // notify the inbound task that a chunk of cache entries was received
         InboundTransferTask inboundTransfer = null;
         synchronized (this) {
            if (stateChunk.getSegmentId() == -1) {
               if (transfersBySource.get(sender) != null) {
                  for (InboundTransferTask transferTask : transfersBySource.get(sender)) {
                     if (transferTask.isDataPlacementTransfer()) {
                        inboundTransfer = transferTask;
                        break;
                     }
                  }
               }
            } else {
               inboundTransfer = transfersBySegment.get(stateChunk.getSegmentId());
            }
         }
         if (inboundTransfer != null) {
            if (stateChunk.getCacheEntries() != null) {
               doApplyState(sender, stateChunk.getSegmentId(), stateChunk.getCacheEntries(), txVersion);
            }

            inboundTransfer.onStateReceived(stateChunk.getSegmentId(), stateChunk.isLastChunk());
         } else {
            log.warnf("Received unsolicited state from node %s for segment %d of cache %s", sender, stateChunk.getSegmentId(), cacheName);
         }
      }

      if (trace) {
         log.tracef("After applying the received state the data container of cache %s has %d keys", cacheName, dataContainer.size(null));
         synchronized (this) {
            log.tracef("Segments not received yet for cache %s: %s", cacheName, transfersBySource);
         }
      }
   }

   private void doApplyState(Address sender, int segmentId, Collection<InternalCacheEntry> cacheEntries, EntryVersion txVersion) {
      if (trace) {
         List<Object> keys = new ArrayList<Object>(cacheEntries.size());
         for (InternalCacheEntry e : cacheEntries) {
            keys.add(e.getKey());
         }
         log.tracef("Received keys %s for segment %d of cache %s from node %s", keys, segmentId, cacheName, sender);
      }

      // CACHE_MODE_LOCAL avoids handling by StateTransferInterceptor and any potential locks in StateTransferLock
      EnumSet<Flag> flags = EnumSet.of(PUT_FOR_STATE_TRANSFER, CACHE_MODE_LOCAL, IGNORE_RETURN_VALUES, SKIP_REMOTE_LOOKUP, SKIP_SHARED_CACHE_STORE, SKIP_OWNERSHIP_CHECK, SKIP_XSITE_BACKUP);
      for (InternalCacheEntry e : cacheEntries) {
         if (configuration.locking().isolationLevel() == IsolationLevel.SERIALIZABLE) {
            OwnersList ownersList = new OwnersList();
            OwnersList existing = oldKeyOwners.putIfAbsent(e.getKey(), ownersList);
            if (existing != null) {
               ownersList = existing;
            }
            ownersList.add(sender);
            //Wait for the commit of the state-transfer transaction
            TransactionInfo transactionInfo;
            synchronized (this) {
               transactionInfo = this.shadowTransactionInfoReceiverMap.get(sender);
            }

            GMUCacheEntryVersion version = null;
            SortedTransactionQueue.TransactionEntry transactionEntry = null;
            if (transactionInfo == null) {
               log.error("Unable to determine a commit version on state transfer");
            } else {

               try {
                  transactionEntry = transactionInfo.waitForTransactionEntry();
                  if(transactionEntry != null)
                     transactionEntry.awaitUntilCommitted();

               } catch (InterruptedException e1) {
                  e1.printStackTrace();
               }

               if(transactionEntry != null && transactionEntry.isCommitted())
                  version = transactionEntry.getNewVersionInDataContainer();

               if(version == null){
                  log.error("Transaction Info for Transaction Entry "+transactionEntry+" is not null but Commit Version is null");
               }
            }

            //Then put in the GMUDataContainer the new data by using the version numbers of the state transfer transaction
            dataContainer.put(e.getKey(), e.getValue(), version, e.getLifespan(), e.getMaxIdle(), true);


         } else {

            try {
               InvocationContext ctx;
               if (transactionManager != null) {
                  // cache is transactional
                  transactionManager.begin();
                  Transaction transaction = transactionManager.getTransaction();
                  ctx = icc.createInvocationContext(transaction);
                  ((TxInvocationContext) ctx).setImplicitTransaction(true);
               } else {
                  // non-tx cache
                  ctx = icc.createSingleKeyNonTxInvocationContext();
               }

               PutKeyValueCommand put = useVersionedPut ?
                     commandsFactory.buildVersionedPutKeyValueCommand(e.getKey(), e.getValue(), e.getLifespan(), e.getMaxIdle(), e.getVersion(), flags)
                     : commandsFactory.buildPutKeyValueCommand(e.getKey(), e.getValue(), e.getLifespan(), e.getMaxIdle(), flags);

               boolean success = false;
               try {
                  interceptorChain.invoke(ctx, put);
                  success = true;
               } finally {
                  if (ctx.isInTxScope()) {
                     if (success) {
                        interceptorChain.invoke(ctx, commandsFactory.buildSetClassCommand("NBST_CLASS"));
                        ((LocalTransaction) ((TxInvocationContext) ctx).getCacheTransaction()).setFromStateTransfer(true);
                        if (configuration.locking().isolationLevel() == IsolationLevel.SERIALIZABLE &&
                              versionGenerator != null && versionGenerator instanceof GMUVersionGenerator &&
                              txVersion != null) {
                           TxInvocationContext tctx = (TxInvocationContext) ctx;
                           EntryVersion currentVersion = tctx.getTransactionVersion();
                           tctx.setTransactionVersion(((GMUVersionGenerator) versionGenerator).mergeAndMax(currentVersion, txVersion));
                        }
                        try {
                           transactionManager.commit();
                        } catch (Throwable ex) {
                           log.errorf(ex, "Could not commit transaction created by state transfer of key %s", e.getKey());
                           if (transactionManager.getTransaction() != null) {
                              transactionManager.rollback();
                           }
                        }
                     } else {
                        transactionManager.rollback();
                     }
                  }
               }
            } catch (Exception ex) {
               log.problemApplyingStateForKey(ex.getMessage(), e.getKey(), ex);
            }
         }
         log.debugf("Finished applying state for segment %d of cache %s", segmentId, cacheName);
      }
   }

   protected void applyTransactions(Address sender, Collection<TransactionInfo> transactions) {
      if (isTransactional) {
         boolean shadowApplied = false;
         for (TransactionInfo transactionInfo : transactions) {
            if (transactionInfo.isShadow()) {
               //Try to commit the Receiver Shadow Transaction. If successful it inserts a new entry in the shadowTransactionInfoReceiverMap
               applyShadowTransaction(sender, transactionInfo);
               shadowApplied = true;
            } else {
               CacheTransaction tx = transactionTable.getLocalTransaction(transactionInfo.getGlobalTransaction());
               if (tx == null) {
                  tx = transactionTable.getRemoteTransaction(transactionInfo.getGlobalTransaction());
                  if (tx == null) {
                     tx = transactionTable.getOrCreateRemoteTransaction(transactionInfo.getGlobalTransaction(), transactionInfo.getModifications());
                     ((RemoteTransaction) tx).setMissingLookedUpEntries(true);
                  }
               }
               for (Object key : transactionInfo.getLockedKeys()) {
                  tx.addBackupLockForKey(key);
               }
            }
         }
         if(configuration.locking().isolationLevel() == IsolationLevel.SERIALIZABLE && !shadowApplied){
            transactionCommitManager.rollbackReceiverStateTransferTransaction(sender);
            gmuExecutorService.checkForReadyTasks();
         }
      }
   }

   // Must run after the CacheLoaderManager
   @Start(priority = 20)
   public void start() {
      isFetchEnabled = configuration.clustering().stateTransfer().fetchInMemoryState() || cacheLoaderManager.isFetchPersistentState();
   }

   @Stop(priority = 20)
   @Override
   public void stop() {
      if (trace) {
         log.tracef("Shutting down StateConsumer of cache %s on node %s", cacheName, rpcManager.getAddress());
      }

      try {
         synchronized (this) {
            // cancel all inbound transfers
            taskQueue.clear();
            for (Iterator<List<InboundTransferTask>> it = transfersBySource.values().iterator(); it.hasNext(); ) {
               List<InboundTransferTask> inboundTransfers = it.next();
               it.remove();
               for (InboundTransferTask inboundTransfer : inboundTransfers) {
                  inboundTransfer.cancel();
               }
            }
            transfersBySource.clear();
            transfersBySegment.clear();
         }
      } catch (Throwable t) {
         log.errorf(t, "Failed to stop StateConsumer of cache %s on node %s", cacheName, rpcManager.getAddress());
      }
   }

   @Override
   public CacheTopology getCacheTopology() {
      return cacheTopology;
   }

   private void applyShadowTransaction(final Address sender, final TransactionInfo transactionInfo) {

      synchronized (this) {
         TransactionInfo info = shadowTransactionInfoReceiverMap.get(sender);
         if(info != null){
            log.warn("Shadow Transaction Info "+transactionInfo+" already in shadowTransactionInfoReceiverMap for sender "+sender+". Old entry is "+info);
         }
         shadowTransactionInfoReceiverMap.put(sender, transactionInfo);
      }

      this.gmuExecutorService.execute(new BlockingRunnable() {
         @Override
         public boolean isReady() {
            return true;
         }
         @Override
         public void run() {

            try {
               SortedTransactionQueue.TransactionEntry transactionEntry = transactionCommitManager.commitReceiverStateTransferTransaction(sender, transactionInfo.getVersion());
               //see org.infinispan.tx.gmu.DistConsistencyTest3.testNoCommitDeadlock
               //the commitTransaction() can re-order the queue. we need to check for pending commit commands.
               //if not, the queue can be blocked forever.
               gmuExecutorService.checkForReadyTasks();

               if (transactionEntry != null && transactionInfo != null) {
                  //Attach the entry in the commit queue to the transaction info
                  transactionInfo.setTransactionEntry(transactionEntry);
               }

               if(transactionEntry != null){
                  //Wait for this transaction is ready to be committed.
                  transactionEntry.awaitUntilIsReadyToCommit();
               }

               if(transactionEntry != null && !transactionEntry.isCommitted()){
                  //If this transaction is not committed try to commit transactions from the commit queue
                  Collection<SortedTransactionQueue.TransactionEntry> transactionsToCommit = transactionCommitManager.getTransactionsToCommit();

                  List<CommandInterceptor> interceptors = interceptorChain.getInterceptorsWithClass(GMUEntryWrappingInterceptor.class);
                  GMUEntryWrappingInterceptor GMUInterceptor = null;
                  if(interceptors == null || interceptors.isEmpty()){
                     log.fatal("No GMUEntryWrappingInterceptor found");
                     return;
                  }
                  else{
                     GMUInterceptor = ((GMUEntryWrappingInterceptor)(interceptors.get(0)));
                  }
                  transactionCommitManager.executeCommit(transactionEntry, transactionsToCommit, null, GMUInterceptor);

               }

            } catch (Throwable throwable) {
               log.fatal("Error while committing transaction", throwable);
               shadowTransactionInfoReceiverMap.remove(sender);
               transactionCommitManager.rollbackReceiverStateTransferTransaction(sender);
            }
            finally{
               gmuExecutorService.checkForReadyTasks();
            }

         }

         @Override
         public final String toString() {
            return "Apply Shadow Transaction Thread. "+transactionInfo;
         }
      });


   }

   private void addTransfers(Set<Integer> segments) {
      if(trace)log.tracef("Adding inbound state transfer for segments %s of cache %s", segments, cacheName);

      // the set of nodes that reported errors when fetching data from them - these will not be retried in this topology
      Set<Address> excludedSources = new HashSet<Address>();

      // the sources and segments we are going to get from each source
      Map<Address, Set<Integer>> sources = new HashMap<Address, Set<Integer>>();

      if (isTransactional && !isTotalOrder()) {
         requestTransactions(segments, sources, excludedSources);
      }

      if (isFetchEnabled) {
         requestSegments(segments, sources, excludedSources);
      }

      if(trace)log.tracef("Finished adding inbound state transfer for segments %s of cache %s", segments, cacheName);
   }

   private void findSources(Set<Integer> segments, Map<Address, Set<Integer>> sources, Set<Address> excludedSources) {
      for (Integer segmentId : segments) {
         Address source = findSource(segmentId, excludedSources);
         // ignore all segments for which there are no other owners to pull data from.
         // these segments are considered empty (or lost) and do not require a state transfer
         if (source != null) {
            Set<Integer> segmentsFromSource = sources.get(source);
            if (segmentsFromSource == null) {
               segmentsFromSource = new HashSet<Integer>();
               sources.put(source, segmentsFromSource);
            }
            segmentsFromSource.add(segmentId);
         }
      }
   }

   private Address findSource(int segmentId, Set<Address> excludedSources) {
      List<Address> owners = cacheTopology.getReadConsistentHash().locateOwnersForSegment(segmentId);
      if (!owners.contains(rpcManager.getAddress())) {
         // iterate backwards because we prefer to fetch from newer nodes
         for (int i = owners.size() - 1; i >= 0; i--) {
            Address o = owners.get(i);
            if (!o.equals(rpcManager.getAddress()) && !excludedSources.contains(o)) {
               return o;
            }
         }
         log.noLiveOwnersFoundForSegment(segmentId, cacheName, owners, excludedSources);
      }
      return null;
   }

   private void requestTransactions(Set<Integer> segments, Map<Address, Set<Integer>> sources, Set<Address> excludedSources) {
      findSources(segments, sources, excludedSources);

      boolean seenFailures = false;
      while (true) {
         Set<Integer> failedSegments = new HashSet<Integer>();
         for (Map.Entry<Address, Set<Integer>> e : sources.entrySet()) {
            Address source = e.getKey();
            Set<Integer> segmentsFromSource = e.getValue();
            List<TransactionInfo> transactions = getTransactions(source, segmentsFromSource, cacheTopology.getTopologyId());
            if (transactions != null) {
               applyTransactions(source, transactions);
            } else {
               // if requesting the transactions failed we need to retry from another source
               failedSegments.addAll(segmentsFromSource);
               excludedSources.add(source);
               if (configuration.locking().isolationLevel() == IsolationLevel.SERIALIZABLE) {
                  transactionCommitManager.rollbackReceiverStateTransferTransaction(source);
               }
            }
         }

         if (failedSegments.isEmpty()) {
            break;
         }

         // look for other sources for all failed segments
         seenFailures = true;
         sources.clear();
         findSources(failedSegments, sources, excludedSources);
      }

      if (seenFailures) {
         // start fresh when next step starts (fetching segments)
         sources.clear();
      }
   }

   protected List<TransactionInfo> getTransactions(Address source, Set<Integer> segments, int topologyId) {
      if (trace) {
         log.tracef("Requesting transactions for segments %s of cache %s from node %s", segments, cacheName, source);
      }
      // get transactions and locks
      try {
         EntryVersion preparedVersion = null;
         if (configuration.locking().isolationLevel() == IsolationLevel.SERIALIZABLE) {
            //Prepare a shadow transaction for this state transfer from sender.
            preparedVersion = transactionCommitManager.prepareReceiverStateTransferTransaction(source);
         }
         StateRequestCommand cmd = commandsFactory.buildStateRequestCommand(StateRequestCommand.Type.GET_TRANSACTIONS, rpcManager.getAddress(), topologyId, segments, preparedVersion);
         Map<Address, Response> responses = rpcManager.invokeRemotely(Collections.singleton(source), cmd, ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS, timeout, false);
         Response response = responses.get(source);
         if (response instanceof SuccessfulResponse) {
            return (List<TransactionInfo>) ((SuccessfulResponse) response).getResponseValue();
         }
         log.failedToRetrieveTransactionsForSegments(segments, cacheName, source, null);
      } catch (CacheException e) {
         log.failedToRetrieveTransactionsForSegments(segments, cacheName, source, e);
      }
      if (configuration.locking().isolationLevel() == IsolationLevel.SERIALIZABLE) {
         transactionCommitManager.rollbackReceiverStateTransferTransaction(source);
         gmuExecutorService.checkForReadyTasks();
      }
      return null;
   }

   private void requestSegments(Set<Integer> segments, Map<Address, Set<Integer>> sources, Set<Address> excludedSources) {
      if (sources.isEmpty()) {
         findSources(segments, sources, excludedSources);
      }

      for (Map.Entry<Address, Set<Integer>> e : sources.entrySet()) {
         addTransfer(e.getKey(), e.getValue());
      }

      startTransferThread(excludedSources);
   }

   protected void startTransferThread(final Set<Address> excludedSources) {
      synchronized (this) {
         if (isTransferThreadRunning) {
            return;
         }
         isTransferThreadRunning = true;
      }

      executorService.submit(new BlockingRunnable() {

         @Override
         public boolean isReady() {
            return true;
         }
         @Override
         public void run() {
            try {
               while (true) {
                  List<InboundTransferTask> failedTasks = new ArrayList<InboundTransferTask>();
                  while (true) {
                     InboundTransferTask task;
                     try {
                        task = taskQueue.pollFirst(200, TimeUnit.MILLISECONDS);
                        if (task == null) {
                           break;
                        }
                     } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                     }

                     if (!task.requestSegments()) {
                        // if requesting the segments failed we'll take care of it later
                        failedTasks.add(task);
                     } else {
                        try {
                           if (!task.awaitCompletion()) {
                              failedTasks.add(task);
                           }
                        } catch (InterruptedException e) {
                           Thread.currentThread().interrupt();
                           return;
                        }
                     }
                  }

                  if (failedTasks.isEmpty() && taskQueue.isEmpty()) {
                     break;
                  }

                  if(trace)log.tracef("Retrying %d failed tasks", failedTasks.size());

                  // look for other sources for the failed segments and replace all failed tasks with new tasks to be retried
                  // remove+add needs to be atomic
                  synchronized (StateConsumerImpl.this) {
                     Set<Integer> failedSegments = new HashSet<Integer>();
                     for (InboundTransferTask task : failedTasks) {
                        if (removeTransfer(task)) {
                           excludedSources.add(task.getSource());
                           failedSegments.addAll(task.getSegments());
                        }
                     }

                     // should re-add only segments we still own and are not already in
                     failedSegments.retainAll(getOwnedSegments(cacheTopology.getWriteConsistentHash()));
                     failedSegments.removeAll(getOwnedSegments(cacheTopology.getReadConsistentHash()));

                     Map<Address, Set<Integer>> sources = new HashMap<Address, Set<Integer>>();
                     findSources(failedSegments, sources, excludedSources);
                     for (Map.Entry<Address, Set<Integer>> e : sources.entrySet()) {
                        addTransfer(e.getKey(), e.getValue());
                     }
                  }
               }
            } finally {
               synchronized (StateConsumerImpl.this) {
                  isTransferThreadRunning = false;
               }
            }
         }

         @Override
         public final String toString() {
            return "Topology Executor Thread";
         }
      });
   }

   /**
    * Cancel transfers for segments we no longer own.
    *
    * @param removedSegments segments to be cancelled
    */
   private void cancelTransfers(Set<Integer> removedSegments) {
      synchronized (this) {
         List<Integer> segmentsToCancel = new ArrayList<Integer>(removedSegments);
         while (!segmentsToCancel.isEmpty()) {
            int segmentId = segmentsToCancel.remove(0);
            InboundTransferTask inboundTransfer = transfersBySegment.get(segmentId);
            if (inboundTransfer != null) { // we need to check the transfer was not already completed
               Set<Integer> cancelledSegments = new HashSet<Integer>(removedSegments);
               cancelledSegments.retainAll(inboundTransfer.getSegments());
               segmentsToCancel.removeAll(cancelledSegments);
               transfersBySegment.keySet().removeAll(cancelledSegments);
               //this will also remove it from transfersBySource if the entire task gets cancelled
               inboundTransfer.cancelSegments(cancelledSegments);
            }
         }
      }
   }

   private void invalidateSegments(ConsistentHash consistentHash, Set<Integer> segmentsToL1) {
      if (configuration.locking().isolationLevel() == IsolationLevel.SERIALIZABLE) {
         return; //cannot remove the old value due to inconsistencies.
      }

      // The actual owners keep track of the nodes that hold a key in L1 ("requestors") and
      // they invalidate the key on every requestor after a change.
      // But this information is only present on the owners where the ClusteredGetKeyValueCommand
      // got executed - if the requestor only contacted one owner, and that node is no longer an owner
      // (perhaps because it left the cluster), the other owners will not know to invalidate the key
      // on that requestor. Furthermore, the requestors list is not copied to the new owners during
      // state transfers.
      // To compensate for this, we delete all L1 entries in segments that changed ownership during
      // this topology update. We can't actually differentiate between L1 entries and regular entries,
      // so we delete all entries that don't belong to this node in the current OR previous topology.
      Set<Object> keysToL1 = new HashSet<Object>();
      Set<Object> keysToRemove = new HashSet<Object>();

      // gather all keys from data container that belong to the segments that are being removed/moved to L1
      for (InternalCacheEntry ice : dataContainer) {
         Object key = ice.getKey();
         int keySegment = getSegment(key);
         if (segmentsToL1.contains(keySegment)) {
            keysToL1.add(key);
         } else if (!consistentHash.isKeyLocalToNode(rpcManager.getAddress(), key)) {
            keysToRemove.add(key);
         }
      }

      // gather all keys from cache store that belong to the segments that are being removed/moved to L1
      CacheStore cacheStore = getCacheStore();
      if (cacheStore != null) {
         //todo [anistor] extend CacheStore interface to be able to specify a filter when loading keys (ie. keys should belong to desired segments)
         try {
            Set<Object> storedKeys = cacheStore.loadAllKeys(new ReadOnlyDataContainerBackedKeySet(dataContainer));
            for (Object key : storedKeys) {
               int keySegment = getSegment(key);
               if (segmentsToL1.contains(keySegment)) {
                  keysToL1.add(key);
               } else if (!consistentHash.isKeyLocalToNode(rpcManager.getAddress(), key)) {
                  keysToRemove.add(key);
               }
            }

         } catch (CacheLoaderException e) {
            log.failedLoadingKeysFromCacheStore(e);
         }
      }

      if (configuration.clustering().l1().onRehash()) {
         log.debugf("Moving to L1 state for segments %s of cache %s", segmentsToL1, cacheName);
      } else {
         log.debugf("Removing state for segments %s of cache %s", segmentsToL1, cacheName);
      }
      if (!keysToL1.isEmpty()) {
         try {
            InvalidateCommand invalidateCmd = commandsFactory.buildInvalidateFromL1Command(true, EnumSet.of(CACHE_MODE_LOCAL, SKIP_LOCKING), keysToL1);
            InvocationContext ctx = icc.createNonTxInvocationContext();
            interceptorChain.invoke(ctx, invalidateCmd);

            log.debugf("Invalidated %d keys, data container now has %d keys", keysToL1.size(), dataContainer.size(null));
            if (trace) log.tracef("Invalidated keys: %s", keysToL1);
         } catch (CacheException e) {
            log.failedToInvalidateKeys(e);
         }
      }

      log.debugf("Removing state for segments not in %s or %s for cache %s", consistentHash, segmentsToL1, cacheName);
      if (!keysToRemove.isEmpty()) {
         try {
            InvalidateCommand invalidateCmd = commandsFactory.buildInvalidateCommand(EnumSet.of(CACHE_MODE_LOCAL, SKIP_LOCKING), keysToRemove.toArray());
            InvocationContext ctx = icc.createNonTxInvocationContext();
            interceptorChain.invoke(ctx, invalidateCmd);

            log.debugf("Invalidated %d keys, data container of cache %s now has %d keys", keysToRemove.size(), cacheName, dataContainer.size(null));
            if (trace) log.tracef("Invalidated keys: %s", keysToRemove);
         } catch (CacheException e) {
            log.failedToInvalidateKeys(e);
         }
      }
   }

   /**
    * Check if any of the existing transfers should be restarted from a different source because the initial source is no longer a member.
    *
    * @param cacheTopology
    * @param addedSegments
    */
   private void restartBrokenTransfers(CacheTopology cacheTopology, Set<Integer> addedSegments) {
      Set<Address> members = new HashSet<Address>(cacheTopology.getReadConsistentHash().getMembers());
      synchronized (this) {
         for (Iterator<Address> it = transfersBySource.keySet().iterator(); it.hasNext(); ) {
            Address source = it.next();
            if (!members.contains(source)) {
               if (trace) {
                  log.tracef("Removing inbound transfers from source %s for cache %s", source, cacheName);
               }
               List<InboundTransferTask> inboundTransfers = transfersBySource.get(source);
               it.remove();
               for (InboundTransferTask inboundTransfer : inboundTransfers) {
                  // these segments will be restarted if they are still in new write CH
                  if (trace) {
                     log.tracef("Removing inbound transfers for segments %s from source %s for cache %s", inboundTransfer.getSegments(), source, cacheName);
                  }
                  taskQueue.remove(inboundTransfer);
                  inboundTransfer.terminate();
                  transfersBySegment.keySet().removeAll(inboundTransfer.getSegments());
                  addedSegments.addAll(inboundTransfer.getUnfinishedSegments());
               }
            }
         }

         // exclude those that are already in progress from a valid source
         addedSegments.removeAll(transfersBySegment.keySet());
      }
   }

   private int getSegment(Object key) {
      // here we can use any CH version because the routing table is not involved in computing the segment
      return cacheTopology.getReadConsistentHash().getSegment(key);
   }

   /**
    * Obtains the CacheStore that will be used for purging segments that are no longer owned by this node.
    * The CacheStore will be purged only if it is enabled and it is not shared.
    */
   private CacheStore getCacheStore() {
      if (cacheLoaderManager.isEnabled() && !cacheLoaderManager.isShared()) {
         return cacheLoaderManager.getCacheStore();
      }
      return null;
   }

   private InboundTransferTask addTransfer(Address source, Set<Integer> segmentsFromSource) {
      synchronized (this) {
         if(trace)log.tracef("Adding transfer from %s for segments %s", source, segmentsFromSource);
         segmentsFromSource.removeAll(transfersBySegment.keySet());  // already in progress segments are excluded
         if (!segmentsFromSource.isEmpty()) {
            InboundTransferTask inboundTransfer = new InboundTransferTask(segmentsFromSource, source,
                                                                          cacheTopology.getTopologyId(), this, rpcManager, commandsFactory, timeout, cacheName);
            for (int segmentId : segmentsFromSource) {
               transfersBySegment.put(segmentId, inboundTransfer);
            }
            List<InboundTransferTask> inboundTransfers = transfersBySource.get(inboundTransfer.getSource());
            if (inboundTransfers == null) {
               inboundTransfers = new ArrayList<InboundTransferTask>();
               transfersBySource.put(inboundTransfer.getSource(), inboundTransfers);
            }
            inboundTransfers.add(inboundTransfer);
            taskQueue.add(inboundTransfer);
            return inboundTransfer;
         } else {
            return null;
         }
      }
   }

   private boolean removeTransfer(InboundTransferTask inboundTransfer) {
      synchronized (this) {
         if(trace)log.tracef("Removing inbound transfers for segments %s from source %s for cache %s",
                    inboundTransfer.getSegments(), inboundTransfer.getSource(), cacheName);
         taskQueue.remove(inboundTransfer);
         List<InboundTransferTask> transfers = transfersBySource.get(inboundTransfer.getSource());
         if (transfers != null) {
            if (transfers.remove(inboundTransfer)) {
               if (transfers.isEmpty()) {
                  transfersBySource.remove(inboundTransfer.getSource());
                  shadowTransactionInfoReceiverMap.remove(inboundTransfer.getSource());
               }
               transfersBySegment.keySet().removeAll(inboundTransfer.getSegments());
               return true;
            }
         }
      }
      return false;
   }

   void onTaskCompletion(InboundTransferTask inboundTransfer) {
      if(trace)log.tracef("Completion of inbound transfer task: %s ", inboundTransfer);
      removeTransfer(inboundTransfer);

      notifyEndOfRebalanceIfNeeded(cacheTopology.getTopologyId());
   }

   private class OwnersList {
      private final List<Address> owners;

      public OwnersList() {
         owners = new LinkedList<Address>();
      }

      public synchronized final void add(Address owner) {
         owners.add(owner);
      }

      public synchronized final boolean contains(Address owner) {
         return owners.contains(owner);
      }

      public synchronized final List<Address> toList() {
         return Collections.unmodifiableList(owners);
      }
   }

   private boolean isTotalOrder() {
      return protocolManager.isTotalOrder();
   }
}
