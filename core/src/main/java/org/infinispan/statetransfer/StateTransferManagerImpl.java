package org.infinispan.statetransfer;

import org.infinispan.Cache;
import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.ConsistentHashFactory;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.distribution.ch.impl.SyncConsistentHashFactory;
import org.infinispan.distribution.ch.impl.SyncReplicatedConsistentHashFactory;
import org.infinispan.distribution.ch.impl.TopologyAwareSyncConsistentHashFactory;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.partitionhandling.impl.PartitionHandlingManager;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.CacheJoinInfo;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.CacheTopologyHandler;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.topology.TopologyState;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * {@link StateTransferManager} implementation.
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
public class StateTransferManagerImpl implements StateTransferManager {

   private static final Log log = LogFactory.getLog(StateTransferManagerImpl.class);
   private static final boolean trace = log.isTraceEnabled();

   private StateConsumer stateConsumer;
   private StateProvider stateProvider;
   private PartitionHandlingManager partitionHandlingManager;
   private String cacheName;
   private CacheNotifier cacheNotifier;
   private Configuration configuration;
   private GlobalConfiguration globalConfiguration;
   private RpcManager rpcManager;
   private LocalTopologyManager localTopologyManager;

   private final CountDownLatch initialStateTransferComplete = new CountDownLatch(1);
   // The first topology in which the local node was a member. Any command with a lower
   // topology id will be ignored.
   private volatile int firstTopologyAsMember = Integer.MAX_VALUE;
   private KeyPartitioner keyPartitioner;
   private volatile TopologyState localState = TopologyState.NONE;

   public StateTransferManagerImpl() {
   }

   @Inject
   public void init(StateConsumer stateConsumer,
                    StateProvider stateProvider,
                    Cache cache,
                    CacheNotifier cacheNotifier,
                    Configuration configuration,
                    GlobalConfiguration globalConfiguration,
                    RpcManager rpcManager,
                    KeyPartitioner keyPartitioner,
                    LocalTopologyManager localTopologyManager,
                    PartitionHandlingManager partitionHandlingManager) {
      this.stateConsumer = stateConsumer;
      this.stateProvider = stateProvider;
      this.cacheName = cache.getName();
      this.cacheNotifier = cacheNotifier;
      this.configuration = configuration;
      this.globalConfiguration = globalConfiguration;
      this.rpcManager = rpcManager;
      this.keyPartitioner = keyPartitioner;
      this.localTopologyManager = localTopologyManager;
      this.partitionHandlingManager = partitionHandlingManager;
   }

   // needs to be AFTER the DistributionManager and *after* the cache loader manager (if any) inits and preloads
   @Start(priority = 60)
   @Override
   public void start() throws Exception {
      if (trace) {
         log.tracef("Starting StateTransferManager of cache %s on node %s", cacheName, rpcManager.getAddress());
      }

      CacheJoinInfo joinInfo = new CacheJoinInfo(pickConsistentHashFactory(),
            configuration.clustering().hash().hash(),
            configuration.clustering().hash().numSegments(),
            configuration.clustering().hash().numOwners(),
            configuration.clustering().stateTransfer().timeout(),
            configuration.transaction().transactionProtocol().isTotalOrder(),
            configuration.clustering().hash().capacityFactor());

      CacheTopology initialTopology = localTopologyManager.join(cacheName, joinInfo, new CacheTopologyHandler() {
         @Override
         public void updateConsistentHash(CacheTopology cacheTopology, ConsistentHash newCH, TopologyState state) {
            switch (localState) {
               case NONE:
                  //we miss the rebalance_start
                  if (state == TopologyState.REBALANCE) {
                     rebalance(cacheTopology, newCH);
                     return;
                  }
                  break;
               case REBALANCE:
                  //we miss the read_ch_update
                  if (state == TopologyState.READ_CH_UPDATE) {
                     updateReadConsistentHash(cacheTopology);
                     return;
                  }
                  break;
               case READ_CH_UPDATE:
                  //we miss the write_ch_update
                  if (state == TopologyState.NONE) {
                     updateWriteConsistentHash(cacheTopology);
                     return;
                  }
                  break;
            }
            CacheTopologyCollection collection = new CacheTopologyCollection(cacheTopology);

            preTopologyUpdate(collection, TopologyUpdate.CH_UPDATE);

            stateConsumer.onConsistentHashUpdate(collection.newTopology);

            posTopologyUpdate(collection);
         }

         @Override
         public void updateReadConsistentHash(CacheTopology cacheTopology) {
            CacheTopologyCollection collection = new CacheTopologyCollection(cacheTopology);

            preTopologyUpdate(collection, TopologyUpdate.READ_CH_UPDATE);

            cacheNotifier.notifyDataRehashed(collection.oldTopology != null ? collection.oldTopology.getReadConsistentHash() : null,
                                             collection.newTopology.getReadConsistentHash(), null,
                                             collection.newTopology.getTopologyId(), false);
            localState = TopologyState.READ_CH_UPDATE;
            stateConsumer.onReadConsistentHashUpdate(collection.newTopology);

            posTopologyUpdate(collection);
         }

         @Override
         public void updateWriteConsistentHash(CacheTopology cacheTopology) {
            CacheTopologyCollection collection = new CacheTopologyCollection(cacheTopology);

            preTopologyUpdate(collection, TopologyUpdate.WRITE_CH_UPDATE);

            localState = TopologyState.NONE;
            stateConsumer.onWriteConsistentHashUpdate(collection.newTopology);

            posTopologyUpdate(collection);
         }

         @Override
         public void rebalance(CacheTopology cacheTopology, ConsistentHash newCH) {
            CacheTopologyCollection collection = new CacheTopologyCollection(cacheTopology);

            preTopologyUpdate(collection, TopologyUpdate.REBALANCE);

            cacheNotifier.notifyDataRehashed(collection.newTopology.getReadConsistentHash(), newCH,
                                             collection.newTopology.getWriteConsistentHash(),
                                             collection.newTopology.getTopologyId(), true);
            localState = TopologyState.REBALANCE;
            stateConsumer.onRebalanceStart(collection.newTopology);

            posTopologyUpdate(collection);
         }
      }, partitionHandlingManager);

      if (trace) {
         log.tracef("StateTransferManager of cache %s on node %s received initial topology %s", cacheName, rpcManager.getAddress(), initialTopology);
      }
   }

   /**
    * If no ConsistentHashFactory was explicitly configured we choose a suitable one based on cache mode.
    */
   private ConsistentHashFactory pickConsistentHashFactory() {
      ConsistentHashFactory factory = configuration.clustering().hash().consistentHashFactory();
      if (factory == null) {
         CacheMode cacheMode = configuration.clustering().cacheMode();
         if (cacheMode.isClustered()) {
            if (cacheMode.isDistributed()) {
               if (globalConfiguration.transport().hasTopologyInfo()) {
                  factory = new TopologyAwareSyncConsistentHashFactory();
               } else {
                  factory = new SyncConsistentHashFactory();
               }
            } else {
               // this is also used for invalidation mode
               factory = new SyncReplicatedConsistentHashFactory();
            }
         }
      }
      return factory;
   }

   /**
    * Decorates the given cache topology to add a key partitioner.
    *
    * The key partitioner may include support for grouping as well.
    */
   private CacheTopology addPartitioner(CacheTopology cacheTopology) {
      return cacheTopology.addPartitioner(keyPartitioner);
   }

   private void checkNewTopology(CacheTopologyCollection collection) {
      if (collection.oldTopology != null && collection.oldTopology.getTopologyId() > collection.newTopology.getTopologyId()) {
         throw new IllegalStateException("Old topology is higher: old=" + collection.oldTopology + ", new=" + collection.newTopology);
      }
   }

   private void setFirstTopologyAsMemberIfAbsent(CacheTopology newCacheTopology) {
      // No need for extra synchronization here, since LocalTopologyManager already serializes topology updates.
      if (firstTopologyAsMember == Integer.MAX_VALUE && newCacheTopology.getMembers().contains(rpcManager.getAddress())) {
         if (trace) {
            log.trace("This is the first topology in which the local node is a member");
         }
         firstTopologyAsMember = newCacheTopology.getTopologyId();
      }
   }

   private void setInitialStateTransferComplete() {
      if (initialStateTransferComplete.getCount() > 0) {
         final CacheTopology cacheTopology = stateConsumer.getCacheTopology();
         boolean isJoined = cacheTopology.getReadConsistentHash().getMembers().contains(rpcManager.getAddress());
         if (isJoined && cacheTopology.isStable() && localState == TopologyState.NONE) {
            initialStateTransferComplete.countDown();
            if (trace) {
               log.tracef("Initial state transfer complete for cache %s on node %s", cacheName, rpcManager.getAddress());
            }
         }
      }
   }

   private void preTopologyUpdate(CacheTopologyCollection collection, TopologyUpdate topologyUpdate) {
      collection.oldTopology = stateConsumer.getCacheTopology();
      checkNewTopology(collection);

      if (trace) {
         log.tracef("Updating with cache topology %s on cache %s (%s)", collection.newTopology, cacheName, topologyUpdate);
      }

      setFirstTopologyAsMemberIfAbsent(collection.newTopology);

      // handle the partitioner
      collection.newTopology = addPartitioner(collection.newTopology);

      notifyTopologyChanged(collection, true);
   }

   private void posTopologyUpdate(CacheTopologyCollection collection) {
      stateProvider.onTopologyUpdate(collection.newTopology);
      notifyTopologyChanged(collection, false);
      setInitialStateTransferComplete();
      partitionHandlingManager.onTopologyUpdate(collection.newTopology);
   }

   private void notifyTopologyChanged(CacheTopologyCollection collection, boolean pre) {
      cacheNotifier.notifyTopologyChanged(collection.oldTopology, collection.newTopology,
                                          collection.newTopology.getTopologyId(), pre);
   }

   @Start(priority = 1000)
   @SuppressWarnings("unused")
   public void waitForInitialStateTransferToComplete() throws Exception {
      if (configuration.clustering().stateTransfer().awaitInitialTransfer()) {
         if (!localTopologyManager.isRebalancingEnabled()) {
            initialStateTransferComplete.countDown();
         }
         if (trace) log.tracef("Waiting for initial state transfer to finish for cache %s on %s", cacheName, rpcManager.getAddress());
         boolean success = initialStateTransferComplete.await(configuration.clustering().stateTransfer().timeout(), TimeUnit.MILLISECONDS);
         if (!success) {
            throw new CacheException(String.format("Initial state transfer timed out for cache %s on %s",
                  cacheName, rpcManager.getAddress()));
         }
      }
   }

   @Stop(priority = 20)
   @Override
   public void stop() {
      if (trace) {
         log.tracef("Shutting down StateTransferManager of cache %s on node %s", cacheName, rpcManager.getAddress());
      }
      initialStateTransferComplete.countDown();
      localTopologyManager.leave(cacheName);
   }

   @Override
   public boolean isJoinComplete() {
      return stateConsumer.getCacheTopology() != null; // TODO [anistor] this does not mean we have received a topology update or a rebalance yet
   }

   @Override
   public String getRebalancingStatus() throws Exception {
      return localTopologyManager.getRebalancingStatus(cacheName).toString();
   }

   @Override
   public boolean isStateTransferInProgress() {
      return stateConsumer.isStateTransferInProgress();
   }

   @Override
   public boolean isStateTransferInProgressForKey(Object key) {
      return stateConsumer.isStateTransferInProgressForKey(key);
   }

   @Override
   public CacheTopology getCacheTopology() {
      return stateConsumer.getCacheTopology();
   }

   @Override
   public Map<Address, Response> forwardCommandIfNeeded(TopologyAffectedCommand command, Set<Object> affectedKeys,
                                                        Address origin) {
      final CacheTopology cacheTopology = getCacheTopology();
      if (cacheTopology == null) {
         if (trace) {
            log.tracef("Not fowarding command %s because topology is null.", command);
         }
         return Collections.emptyMap();
      }
      int cmdTopologyId = command.getTopologyId();
      // forward commands with older topology ids to their new targets
      // but we need to make sure we have the latest topology
      int localTopologyId = cacheTopology.getTopologyId();
      // if it's a tx/lock/write command, forward it to the new owners
      if (trace) {
         log.tracef("CommandTopologyId=%s, localTopologyId=%s", cmdTopologyId, localTopologyId);
      }

      if (cmdTopologyId < localTopologyId) {
         ConsistentHash writeCh = cacheTopology.getWriteConsistentHash();
         Set<Address> newTargets = new HashSet<>(writeCh.locateAllOwners(affectedKeys));
         newTargets.remove(rpcManager.getAddress());
         // Forwarding to the originator would create a cycle
         // TODO This may not be the "real" originator, but one of the original recipients
         // or even one of the nodes that one of the original recipients forwarded the command to.
         // In non-transactional caches, the "real" originator keeps a lock for the duration
         // of the RPC, so this means we could get a deadlock while forwarding to it.
         newTargets.remove(origin);
         if (!newTargets.isEmpty()) {
            // Update the topology id to prevent cycles
            command.setTopologyId(localTopologyId);
            if (trace) {
               log.tracef("Forwarding command %s to new targets %s", command, newTargets);
            }
            final RpcOptions rpcOptions = rpcManager.getDefaultRpcOptions(false, DeliverOrder.NONE);
            // TODO find a way to forward the command async if it was received async
            // TxCompletionNotificationCommands are the only commands forwarded asynchronously, and they must be OOB
            return rpcManager.invokeRemotely(newTargets, command, rpcOptions);
         }
      }
      return Collections.emptyMap();
   }

   @Override
   public void notifyEndOfRebalance(int topologyId, int rebalanceId) {
      localTopologyManager.confirmRebalance(cacheName, topologyId, rebalanceId, null);
   }

   // TODO Investigate merging ownsData() and getFirstTopologyAsMember(), as they serve a similar purpose
   @Override
   public boolean ownsData() {
      return stateConsumer.ownsData();
   }

   @Override
   public int getFirstTopologyAsMember() {
      return firstTopologyAsMember;
   }

   @Override

   public String toString() {
      return "StateTransferManagerImpl [" + cacheName + "@" + rpcManager.getAddress() + "]";
   }

   @Override
   public TopologyState getTopologyState() {
      return localState;
   }

   private enum TopologyUpdate {
      REBALANCE, READ_CH_UPDATE, WRITE_CH_UPDATE, CH_UPDATE
   }

   private static class CacheTopologyCollection {
      private CacheTopology oldTopology;
      private CacheTopology newTopology;

      public CacheTopologyCollection(CacheTopology newTopology) {
         this.newTopology = newTopology;
      }
   }
}
