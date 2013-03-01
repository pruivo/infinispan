/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.dataplacement;

import org.infinispan.Cache;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.remote.DataPlacementCommand;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.dataplacement.lookup.ObjectLookup;
import org.infinispan.dataplacement.lookup.ObjectLookupFactory;
import org.infinispan.dataplacement.stats.AccessesMessageSizeTask;
import org.infinispan.dataplacement.stats.CheckKeysMovedTask;
import org.infinispan.dataplacement.stats.SaveStatsTask;
import org.infinispan.dataplacement.stats.Stats;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.group.GroupManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.Parameter;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.notifications.cachelistener.annotation.DataRehashed;
import org.infinispan.notifications.cachelistener.event.DataRehashedEvent;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.stats.topK.StreamLibContainer;
import org.infinispan.topology.RebalancePolicy;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/**
 * Manages all phases in the dara placement protocol
 *
 * @author Zhongmiao Li
 * @author João Paiva
 * @author Pedro Ruivo
 * @since 5.2
 */
@MBean(objectName = "DataPlacementManager", description = "Manages the placement of the keys to support a better" +
      " performance in distributed mode")
@Listener
public class DataPlacementManager {

   private static final Log log = LogFactory.getLog(DataPlacementManager.class);
   public static final int INITIAL_COOL_DOWN_TIME = 30000; //30 seconds
   private final AccessesManager accessesManager;
   private final ObjectPlacementManager objectPlacementManager;
   private final ObjectLookupManager objectLookupManager;
   private final RoundManager roundManager;
   private final ExecutorService statsAsync = Executors.newSingleThreadExecutor();
   private RpcManager rpcManager;
   private CommandsFactory commandsFactory;
   private Cache cache;
   private Boolean expectPre = true;
   private ObjectLookupFactory objectLookupFactory;
   private Stats stats;
   private DistributionManager distributionManager;
   private DataPlacementRebalancePolicy rebalancePolicy;

   public DataPlacementManager() {
      roundManager = new RoundManager(INITIAL_COOL_DOWN_TIME);
      accessesManager = new AccessesManager();
      objectPlacementManager = new ObjectPlacementManager();
      objectLookupManager = new ObjectLookupManager();
   }

   @Inject
   public void inject(CommandsFactory commandsFactory, DistributionManager distributionManager, RpcManager rpcManager,
                      Cache cache, StateTransferManager stateTransfer, CacheNotifier cacheNotifier,
                      Configuration configuration, GroupManager groupManager, RebalancePolicy rebalancePolicy) {
      this.rpcManager = rpcManager;
      this.commandsFactory = commandsFactory;
      this.cache = cache;
      this.distributionManager = distributionManager;
      if (rebalancePolicy instanceof DataPlacementRebalancePolicy) {
         this.rebalancePolicy = (DataPlacementRebalancePolicy) rebalancePolicy;
      } else {
         this.rebalancePolicy = null;
      }

      if (!configuration.dataPlacement().enabled()) {
         log.info("Data placement not enabled in Configuration");
         return;
      } else if (this.rebalancePolicy == null) {
         log.info("Data placement not enabled because the Rebalance Policy is not the expected: " +
                        rebalancePolicy.getClass().getSimpleName());
      }

      objectLookupFactory = configuration.dataPlacement().objectLookupFactory();
      objectLookupFactory.setConfiguration(configuration);

      roundManager.init(configuration.dataPlacement().coolDownTime());

      //this is needed because the custom statistics invokes this method twice. the seconds time, it replaces
      //the original object placement and remote accesses manager (== problems!!)
      synchronized (this) {
         if (roundManager.isEnabled()) {
            log.info("Data Placement is already enabled!");
         } else if (configuration.clustering().cacheMode().isDistributed()) {
            accessesManager.setMaxNumberOfKeysToRequest(configuration.dataPlacement().maxNumberOfKeysToRequest());
            accessesManager.setGroupManager(groupManager);
            roundManager.enable();
            cacheNotifier.addListener(this);
            log.info("Data placement enabled");
         } else {
            log.info("Data placement disabled. Not in Distributed mode");
         }
      }
   }

   @Start
   public void start() {
      accessesManager.setStreamLibContainer(StreamLibContainer.getOrCreateStreamLibContainer(cache));
   }

   /**
    * starts a new round of data placement protocol
    *
    * @param newRoundId the new round id
    * @param members    the current cluster members
    */
   public final void startDataPlacement(long newRoundId, Address[] members) {
      if (log.isTraceEnabled()) {
         log.tracef("Start data placement protocol with round %s", newRoundId);
      }
      stats = new Stats(newRoundId, objectLookupFactory.getNumberOfQueryProfilingPhases());

      ClusterSnapshot roundClusterSnapshot = new ClusterSnapshot(members, distributionManager.getConsistentHash().getHashFunction());

      if (!roundClusterSnapshot.contains(rpcManager.getAddress())) {
         log.warnf("Data placement start received but I [%s] am not in the member list %s", rpcManager.getAddress(),
                   roundClusterSnapshot);
         return;
      }

      ConsistentHash consistentHash = distributionManager.getConsistentHash();
      accessesManager.resetState(roundClusterSnapshot, consistentHash);
      objectPlacementManager.resetState(roundClusterSnapshot, consistentHash);
      objectLookupManager.resetState(roundClusterSnapshot, consistentHash.getNumSegments());
      if (!roundManager.startNewRound(newRoundId, roundClusterSnapshot, rpcManager.getAddress())) {
         log.info("Data placement not started!");
         return;
      }
      new Thread("Data-Placement-Thread") {
         @Override
         public void run() {
            try {
               sendRequestToAll();
            } catch (Exception e) {
               log.errorf(e, "Exception caught while starting data placement");
            }
         }
      }.start();
   }

   /**
    * collects all the request list from other members with the object that they want. when all requests are received it
    * decides to each member the object should go and it broadcast the Object Lookup
    *
    * @param sender        the sender
    * @param objectRequest the request list
    * @param roundId       the round id
    */
   public final void addRequest(Address sender, ObjectRequest objectRequest, long roundId) {
      if (log.isDebugEnabled()) {
         log.debugf("Keys request received from %s in round %s", sender, roundId);
      }

      if (!roundManager.ensure(roundId, sender)) {
         log.warn("Not possible to process key request list");
         return;
      }

      if (objectPlacementManager.aggregateRequest(sender, objectRequest)) {
         stats.receivedAccesses();
         Collection<SegmentMapping> objectsToMove = objectPlacementManager.calculateObjectsToMove();

         if (log.isTraceEnabled()) {
            log.tracef("All keys request list received. Object to move are " + objectsToMove);
         }

         //saveObjectsToMoveToFile(objectsToMove);
         Map<Integer, ObjectLookup> segmentObjectLookup = new HashMap<Integer, ObjectLookup>(objectsToMove.size());
         int numberOfOwners = objectPlacementManager.getNumberOfOwners();

         long start = System.nanoTime();

         for (SegmentMapping segmentMapping : objectsToMove) {
            ObjectLookup objectLookup = objectLookupFactory.createObjectLookup(segmentMapping, numberOfOwners);
            if (objectLookup == null) {
               log.errorf("Object lookup created is null for segment " + segmentMapping.getSegmentId());
            }
            segmentObjectLookup.put(segmentMapping.getSegmentId(), objectLookup);
         }

         stats.setObjectLookupCreationDuration(System.nanoTime() - start);

         //statsAsync.submit(new ObjectLookupTask(objectsToMove, objectLookup, stats));

         if (log.isDebugEnabled()) {
            log.debugf("Created %s bloom filters and machine learner rules for each key", objectPlacementManager.getNumberOfOwners());
         }

         stats.calculatedNewOwners();

         if (rpcManager.getTransport().isCoordinator()) {
            addObjectLookup(rpcManager.getAddress(), segmentObjectLookup, roundId);
         } else {
            DataPlacementCommand command = commandsFactory.buildDataPlacementCommand(DataPlacementCommand.Type.OBJECT_LOOKUP_PHASE,
                                                                                     roundManager.getCurrentRoundId());
            command.setObjectLookup(segmentObjectLookup);

            rpcManager.invokeRemotely(Collections.singleton(rpcManager.getTransport().getCoordinator()), command, false, false);
         }
      }
   }

   /**
    * collects all the Object Lookup for each member. when all Object Lookup are collected, it sends an ack for the
    * coordinator
    *
    * @param sender        the sender
    * @param objectLookups the object lookup
    * @param roundId       the round id
    */
   public final void addObjectLookup(Address sender, Map<Integer, ObjectLookup> objectLookups, long roundId) {
      if (log.isDebugEnabled()) {
         log.debugf("Remote Object Lookup received from %s in round %s", sender, roundId);
      }

      if (!roundManager.ensure(roundId, sender)) {
         log.warn("Not possible to process remote Object Lookup");
         return;
      }

      objectLookupFactory.init(objectLookups.values());
      if (objectLookupManager.addObjectLookup(sender, objectLookups)) {
         try {
            rebalancePolicy.setNewSegmentMappings(cache.getName(), objectLookupManager.getClusterObjectLookup());
         } catch (Exception e) {
            log.error("Error triggering State Transfer with the new mappings", e);
         }
      }
   }

   /**
    * sets the cool down time
    *
    * @param milliseconds the new time in milliseconds
    */
   public final void internalSetCoolDownTime(int milliseconds) {
      roundManager.setCoolDownTime(milliseconds);
   }

   public final void handleNewReplicationDegree(int replicationDegree) throws Exception {
      if (replicationDegree > 0) {
         rebalancePolicy.setNewReplicationDegree(cache.getName(), replicationDegree);
         return;
      }
      throw new Exception("Replication Degree should be higher than 0");
   }

   @SuppressWarnings("unchecked")
   @DataRehashed
   public final void keyMovementTest(DataRehashedEvent event) {
      if (log.isTraceEnabled()) {
         log.trace("Data rehashed event trigger");
      }
      log.errorf("Data Rehash Event triggered");
      if (event.getMembersAtEnd().size() == event.getMembersAtStart().size() && stats != null) {
         if (log.isTraceEnabled()) {
            log.tracef("Membership didn't change. may be key movement! Is pre? %s (%s)", event.isPre(), expectPre);
         }
         if (event.isPre() && expectPre) {
            log.errorf("Start State Transfer");
            stats.startStateTransfer();
            expectPre = false;
         } else if (!event.isPre() && !expectPre) {
            log.errorf("End State Transfer");
            stats.endStateTransfer();
            statsAsync.submit(new CheckKeysMovedTask(event.getKeysMoved(), objectPlacementManager, stats,
                                                     accessesManager.getDefaultConsistentHash(), rpcManager.getAddress()));
            statsAsync.submit(new SaveStatsTask(stats));
            expectPre = true;
            roundManager.markRoundFinished();
         }
      }
   }

   @ManagedOperation(description = "Start the data placement algorithm in order to optimize the system performance",
   displayName = "Trigger Data Placement")
   public final void dataPlacementRequest() throws Exception {
      if (!rpcManager.getTransport().isCoordinator()) {
         if (log.isTraceEnabled()) {
            log.trace("Data placement request. Sending request to coordinator");
         }
         DataPlacementCommand command = commandsFactory.buildDataPlacementCommand(DataPlacementCommand.Type.DATA_PLACEMENT_REQUEST,
                                                                                  roundManager.getCurrentRoundId());
         rpcManager.invokeRemotely(Collections.singleton(rpcManager.getTransport().getCoordinator()),
                                   command, false, false);
         return;
      }

      if (rpcManager.getTransport().getMembers().size() == 1) {
         log.warn("Data placement request received but we are the only member. ignoring...");
         return;
      }

      if (rebalancePolicy == null) {
         log.error("Cannot start Data Placement protocol. No Rebalance Policy set!");
         return;
      }

      if (log.isTraceEnabled()) {
         log.trace("Data placement request received.");
      }

      long newRoundId = roundManager.getNewRoundId();
      DataPlacementCommand command = commandsFactory.buildDataPlacementCommand(DataPlacementCommand.Type.DATA_PLACEMENT_START,
                                                                               newRoundId);
      Collection<Address> members = rpcManager.getTransport().getMembers();
      Address[] addressArray = members.toArray(new Address[members.size()]);
      command.setMembers(addressArray);
      rpcManager.broadcastRpcCommand(command, false, false);
      startDataPlacement(newRoundId, addressArray);
   }

   @ManagedAttribute(description = "The cache name", writable = false, displayName = "Cache name")
   public final String getCacheName() {
      return cache.getName();
   }

   @ManagedAttribute(description = "The current cool down time between rounds", writable = false,
   displayName = "Cool Down Time")
   public final long getCoolDownTime() {
      return roundManager.getCoolDownTime();
   }

   @ManagedOperation(description = "Updates the cool down time between two or more data placement requests",
   displayName = "Set cool down time")
   public final void setCoolDownTime(@Parameter(name = "Cool Down Time") int milliseconds) {
      if (log.isTraceEnabled()) {
         log.tracef("Setting new cool down period to %s milliseconds", milliseconds);
      }
      DataPlacementCommand command = commandsFactory.buildDataPlacementCommand(DataPlacementCommand.Type.SET_COOL_DOWN_TIME,
                                                                               roundManager.getCurrentRoundId());
      command.setIntValue(milliseconds);
      rpcManager.broadcastRpcCommand(command, false, false);
      internalSetCoolDownTime(milliseconds);
   }

   @ManagedAttribute(description = "The current round Id", writable = false, displayName = "Current Round ID")
   public final long getCurrentRoundId() {
      return roundManager.getCurrentRoundId();
   }

   @ManagedAttribute(description = "Check if a data placement round is in progress", writable = false,
                     displayName = "Round in progress?")
   public final boolean isRoundInProgress() {
      return roundManager.isRoundInProgress();
   }

   @ManagedAttribute(description = "Check if the data placement is enabled", writable = false, displayName = "Enabled?")
   public final boolean isEnabled() {
      return roundManager.isEnabled();
   }

   @ManagedAttribute(description = "The Object Lookup Factory class name", writable = false,
   displayName = "Object lookup factory class name")
   public final String getObjectLookupFactoryClassName() {
      return objectLookupFactory == null ? "N/A" : objectLookupFactory.getClass().getCanonicalName();
   }

   @ManagedAttribute(description = "The max number of keys to request in each round", writable = false,
   displayName = "Maximum number of keys to request")
   public final int getMaxNumberOfKeysToRequest() {
      return accessesManager == null ? 0 : accessesManager.getMaxNumberOfKeysToRequest();
   }

   @ManagedOperation(description = "Sets a new value (if higher than zero) for the max number of keys to request in " +
         "each round", displayName = "Set maximum number of keys to request")
   public final void setMaxNumberOfKeysToRequest(@Parameter(name = "Keys to request")int value) {
      if (accessesManager != null) {
         accessesManager.setMaxNumberOfKeysToRequest(value);
      }
   }

   @ManagedOperation(description = "Triggers the replication degree optimizer",
                     displayName = "Trigger Replication Degree Optimizer")
   public final void setReplicationDegree(@Parameter(name = "Replication Degree") int replicationDegree) throws Exception {

      if (!rpcManager.getTransport().isCoordinator()) {
         if (log.isTraceEnabled()) {
            log.trace("Replication Degree request received. Sending request to coordinator");
         }
         DataPlacementCommand command = commandsFactory.buildDataPlacementCommand(DataPlacementCommand.Type.REPLICATION_DEGREE,
                                                                                  -1);
         command.setIntValue(replicationDegree);
         rpcManager.invokeRemotely(Collections.singleton(rpcManager.getTransport().getCoordinator()), command, false, 
                                   false);
         return;
      }
      roundManager.replicationDegreeRequest();
      handleNewReplicationDegree(replicationDegree);
   }

   /**
    * obtains the request list to send for each member and sends it
    */
   private void sendRequestToAll() {
      if (log.isTraceEnabled()) {
         log.trace("Start sending keys request");
      }

      accessesManager.calculateAccesses();
      statsAsync.submit(new AccessesMessageSizeTask(stats, accessesManager));

      stats.collectedAccesses();
      for (Address address : rpcManager.getTransport().getMembers()) {
         ObjectRequest request = accessesManager.getObjectRequestForAddress(address);

         if (address.equals(rpcManager.getAddress())) {
            addRequest(address, request, roundManager.getCurrentRoundId());
         } else {
            DataPlacementCommand command = commandsFactory.buildDataPlacementCommand(DataPlacementCommand.Type.REMOTE_TOP_LIST_PHASE,
                                                                                     roundManager.getCurrentRoundId());
            command.setObjectRequest(request);
            rpcManager.invokeRemotely(Collections.singleton(address), command, false, false);
            if (log.isDebugEnabled()) {
               log.debugf("Sending request list objects to %s. Request is %s", address, request.toString(log.isTraceEnabled()));
            }
         }
      }
   }
}
