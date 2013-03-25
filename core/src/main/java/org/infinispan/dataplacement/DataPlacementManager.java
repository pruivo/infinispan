/*
 * INESC-ID, Instituto de Engenharia de Sistemas e Computadores Investigação e Desevolvimento em Lisboa
 * Copyright 2013 INESC-ID and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 3.0 of
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
package org.infinispan.dataplacement;

import org.infinispan.Cache;
import org.infinispan.cacheviews.CacheViewsManager;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.remote.DataPlacementCommand;
import org.infinispan.commons.hash.Hash;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.dataplacement.lookup.ObjectLookup;
import org.infinispan.dataplacement.lookup.ObjectLookupFactory;
import org.infinispan.dataplacement.stats.AccessesMessageSizeTask;
import org.infinispan.dataplacement.stats.CheckKeysMovedTask;
import org.infinispan.dataplacement.stats.ObjectLookupTask;
import org.infinispan.dataplacement.stats.SaveStatsTask;
import org.infinispan.dataplacement.stats.Stats;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.notifications.cachelistener.annotation.DataRehashed;
import org.infinispan.notifications.cachelistener.event.DataRehashedEvent;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.DistributedStateTransferManagerImpl;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.util.Collection;
import java.util.Collections;
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

   public static final int INITIAL_COOL_DOWN_TIME = 30000; //30 seconds
   private static final boolean SAVE = false;
   private static final Log log = LogFactory.getLog(DataPlacementManager.class);
   private final RoundManager roundManager;
   private final ExecutorService statsAsync = Executors.newSingleThreadExecutor();
   private RpcManager rpcManager;
   private CommandsFactory commandsFactory;
   private CacheViewsManager cacheViewsManager;
   private Hash hashFunction;
   private String cacheName;
   private int defaultNumberOfOwners;
   private Boolean expectPre = true;
   private AccessesManager accessesManager;
   private ObjectPlacementManager objectPlacementManager;
   private ObjectLookupManager objectLookupManager;
   private ObjectLookupFactory objectLookupFactory;
   private Stats stats;

   public DataPlacementManager() {
      roundManager = new RoundManager(INITIAL_COOL_DOWN_TIME);
   }

   @Inject
   public void inject(CommandsFactory commandsFactory, DistributionManager distributionManager, RpcManager rpcManager,
                      CacheViewsManager cacheViewsManager, Cache cache, StateTransferManager stateTransfer,
                      CacheNotifier cacheNotifier, Configuration configuration) {
      this.rpcManager = rpcManager;
      this.commandsFactory = commandsFactory;
      this.cacheViewsManager = cacheViewsManager;
      this.cacheName = cache.getName();
      this.hashFunction = configuration.clustering().hash().hash();

      if (!configuration.dataPlacement().enabled()) {
         log.info("Data placement not enabled in Configuration");
         return;
      }

      objectLookupFactory = configuration.dataPlacement().objectLookupFactory();
      objectLookupFactory.setConfiguration(configuration);

      roundManager.setCoolDownTime(configuration.dataPlacement().coolDownTime());

      //this is needed because the custom statistics invokes this method twice. the seconds time, it replaces
      //the original object placement and remote accesses manager (== problems!!)
      synchronized (this) {
         if (stateTransfer instanceof DistributedStateTransferManagerImpl && !roundManager.isEnabled()) {
            defaultNumberOfOwners = configuration.clustering().hash().numOwners();
            accessesManager = new AccessesManager(distributionManager,
                                                  configuration.dataPlacement().maxNumberOfKeysToRequest());
            objectPlacementManager = new ObjectPlacementManager(distributionManager,
                                                                configuration.clustering().hash().hash(),
                                                                defaultNumberOfOwners);
            objectLookupManager = new ObjectLookupManager((DistributedStateTransferManagerImpl) stateTransfer);
            roundManager.enable();
            cacheNotifier.addListener(this);
            log.info("Data placement enabled");
         } else {
            log.info("Data placement disabled. Not in Distributed mode");
         }
      }

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

      ClusterSnapshot roundClusterSnapshot = new ClusterSnapshot(members, hashFunction);

      if (!roundClusterSnapshot.contains(rpcManager.getAddress())) {
         log.warnf("Data placement start received but I [%s] am not in the member list %s", rpcManager.getAddress(),
                   roundClusterSnapshot);
         return;
      }

      objectPlacementManager.resetState(roundClusterSnapshot);
      objectLookupManager.resetState(roundClusterSnapshot);
      accessesManager.resetState(roundClusterSnapshot);
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
         Map<Object, OwnersInfo> objectsToMove = objectPlacementManager.calculateObjectsToMove();

         if (log.isTraceEnabled()) {
            log.tracef("All keys request list received. Object to move are " + objectsToMove);
         }

         saveObjectsToMoveToFile(objectsToMove);

         long start = System.nanoTime();
         ObjectLookup objectLookup = objectLookupFactory.createObjectLookup(objectsToMove, defaultNumberOfOwners);

         if (objectLookup == null) {
            log.errorf("Object lookup created is null");
         }

         stats.setObjectLookupCreationDuration(System.nanoTime() - start);

         statsAsync.submit(new ObjectLookupTask(objectsToMove, objectLookup, stats));

         if (log.isDebugEnabled()) {
            log.debugf("Created %s bloom filters and machine learner rules for each key", defaultNumberOfOwners);
         }

         stats.calculatedNewOwners();
         DataPlacementCommand command = commandsFactory.buildDataPlacementCommand(DataPlacementCommand.Type.OBJECT_LOOKUP_PHASE,
                                                                                  roundManager.getCurrentRoundId());
         command.setObjectLookup(objectLookup);

         rpcManager.broadcastRpcCommand(command, false, false);
         addObjectLookup(rpcManager.getAddress(), objectLookup, roundId);
      }
   }

   /**
    * collects all the Object Lookup for each member. when all Object Lookup are collected, it sends an ack for the
    * coordinator
    *
    * @param sender       the sender
    * @param objectLookup the object lookup
    * @param roundId      the round id
    */
   public final void addObjectLookup(Address sender, ObjectLookup objectLookup, long roundId) {
      if (log.isDebugEnabled()) {
         log.debugf("Remote Object Lookup received from %s in round %s", sender, roundId);
      }

      if (!roundManager.ensure(roundId, sender)) {
         log.warn("Not possible to process remote Object Lookup");
         return;
      }

      objectLookupFactory.init(objectLookup);
      if (objectLookupManager.addObjectLookup(sender, objectLookup)) {
         stats.receivedObjectLookup();
         if (log.isTraceEnabled()) {
            log.tracef("All remote Object Lookup received. Send Ack to coordinator");
         }
         DataPlacementCommand command = commandsFactory.buildDataPlacementCommand(DataPlacementCommand.Type.ACK_COORDINATOR_PHASE,
                                                                                  roundId);
         if (rpcManager.getTransport().isCoordinator()) {
            addAck(roundId, rpcManager.getAddress());
         } else {
            rpcManager.invokeRemotely(Collections.singleton(rpcManager.getTransport().getCoordinator()), command, false);
         }
      }
   }

   /**
    * collects all acks from all members. when all acks are collects, the state transfer is triggered
    *
    * @param roundId the round id
    * @param sender  the sender
    */
   public final void addAck(long roundId, Address sender) {
      if (log.isDebugEnabled()) {
         log.debugf("Ack received in round %s", roundId);
      }

      if (!roundManager.ensure(roundId, sender)) {
         log.warn("Not possible to process Ack");
         return;
      }

      if (objectLookupManager.addAck(sender)) {
         stats.receivedAcks();
         if (log.isTraceEnabled()) {
            log.tracef("All Acks received. Trigger state transfer");
         }
         cacheViewsManager.handleRequestMoveKeys(cacheName);
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
         cacheViewsManager.handleReplicationDegree(cacheName, replicationDegree);
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

   @ManagedOperation(description = "Start the data placement algorithm in order to optimize the system performance")
   public final void dataPlacementRequest() throws Exception {
      if (!rpcManager.getTransport().isCoordinator()) {
         if (log.isTraceEnabled()) {
            log.trace("Data placement request. Sending request to coordinator");
         }
         DataPlacementCommand command = commandsFactory.buildDataPlacementCommand(DataPlacementCommand.Type.DATA_PLACEMENT_REQUEST,
                                                                                  roundManager.getCurrentRoundId());
         rpcManager.invokeRemotely(Collections.singleton(rpcManager.getTransport().getCoordinator()),
                                   command, false);
         return;
      }

      if (rpcManager.getTransport().getMembers().size() == 1) {
         log.warn("Data placement request received but we are the only member. ignoring...");
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

   @ManagedOperation()
   public final void setReplicationDegree(int replicationDegree) throws Exception {

      if (!rpcManager.getTransport().isCoordinator()) {
         if (log.isTraceEnabled()) {
            log.trace("Replication Degree request received. Sending request to coordinator");
         }
         DataPlacementCommand command = commandsFactory.buildDataPlacementCommand(DataPlacementCommand.Type.REPLICATION_DEGREE,
                                                                                  -1);
         command.setIntValue(replicationDegree);
         rpcManager.invokeRemotely(Collections.singleton(rpcManager.getTransport().getCoordinator()),
                                   command, false);
         return;
      }

      if (log.isTraceEnabled()) {
         log.trace("Replication Degree request receive");
      }

      roundManager.replicationDegreeRequest();
      handleNewReplicationDegree(replicationDegree);
   }

   @ManagedAttribute(description = "The cache name", writable = false)
   public final String getCacheName() {
      return cacheName;
   }

   @ManagedAttribute(description = "The current cool down time between rounds", writable = false)
   public final long getCoolDownTime() {
      return roundManager.getCoolDownTime();
   }

   @ManagedOperation(description = "Updates the cool down time between two or more data placement requests")
   public final void setCoolDownTime(int milliseconds) {
      if (log.isTraceEnabled()) {
         log.tracef("Setting new cool down period to %s milliseconds", milliseconds);
      }
      DataPlacementCommand command = commandsFactory.buildDataPlacementCommand(DataPlacementCommand.Type.SET_COOL_DOWN_TIME,
                                                                               roundManager.getCurrentRoundId());
      command.setIntValue(milliseconds);
      rpcManager.broadcastRpcCommand(command, false, false);
      internalSetCoolDownTime(milliseconds);
   }

   @ManagedAttribute(description = "The current round Id", writable = false)
   public final long getCurrentRoundId() {
      return roundManager.getCurrentRoundId();
   }

   @ManagedAttribute(description = "Check if a data placement round is in progress", writable = false)
   public final boolean isRoundInProgress() {
      return roundManager.isRoundInProgress();
   }

   @ManagedAttribute(description = "Check if the data placement is enabled", writable = false)
   public final boolean isEnabled() {
      return roundManager.isEnabled();
   }

   @ManagedAttribute(description = "The Object Lookup Factory class name", writable = false)
   public final String getObjectLookupFactoryClassName() {
      return objectLookupFactory == null ? "N/A" : objectLookupFactory.getClass().getCanonicalName();
   }

   @ManagedAttribute(description = "The max number of keys to request in each round", writable = false)
   public final int getMaxNumberOfKeysToRequest() {
      return accessesManager == null ? 0 : accessesManager.getMaxNumberOfKeysToRequest();
   }

   @ManagedOperation(description = "Sets a new value (if higher than zero) for the max number of keys to request in " +
         "each round")
   public final void setMaxNumberOfKeysToRequest(int value) {
      if (accessesManager != null) {
         accessesManager.setMaxNumberOfKeysToRequest(value);
      }
   }

   private void saveObjectsToMoveToFile(Map<Object, OwnersInfo> ownersInfoMap) {
      if (SAVE) {
         try {
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(
                  new FileOutputStream("round-" + roundManager.getCurrentRoundId()));
            objectOutputStream.writeObject(ownersInfoMap);
            objectOutputStream.flush();
            objectOutputStream.close();
         } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
         }
      }
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
            rpcManager.invokeRemotely(Collections.singleton(address), command, false);
            if (log.isDebugEnabled()) {
               log.debugf("Sending request list objects to %s. Request is %s", address, request.toString(log.isTraceEnabled()));
            }
         }
      }
   }
}
