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

import org.infinispan.dataplacement.ch.ConsistentHashChanges;
import org.infinispan.dataplacement.ch.DataPlacementConsistentHash;
import org.infinispan.dataplacement.ch.ExternalLCRDMappingEntry;
import org.infinispan.dataplacement.ch.LCRDCluster;
import org.infinispan.dataplacement.ch.LCRDConsistentHash;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.jmx.annotations.Parameter;
import org.infinispan.topology.ClusterCacheStatus;
import org.infinispan.topology.ClusterTopologyManager;
import org.infinispan.topology.RebalancePolicy;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Data Placement Optimization rebalance policy
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class DataPlacementRebalancePolicy implements RebalancePolicy {

   private static final Log log = LogFactory.getLog(DataPlacementRebalancePolicy.class);
   private final Map<String, ConsistentHashChanges> consistentHashChangesMap;
   private final Object lock = new Object();
   private ClusterTopologyManager clusterTopologyManager;
   private Map<String, ConsistentHash> cachesPendingRebalance = null;

   public DataPlacementRebalancePolicy() {
      consistentHashChangesMap = new HashMap<String, ConsistentHashChanges>();
   }

   @Inject
   public void setClusterTopologyManager(ClusterTopologyManager clusterTopologyManager) {
      this.clusterTopologyManager = clusterTopologyManager;
   }

   @Override
   public void initCache(String cacheName, ClusterCacheStatus cacheStatus) throws Exception {
      //nothing to do
   }

   @Override
   public void updateCacheStatus(String cacheName, ClusterCacheStatus cacheStatus) throws Exception {
      log.tracef("Cache %s status changed: joiners=%s, topology=%s", cacheName, cacheStatus.getJoiners(),
                 cacheStatus.getCacheTopology());
      if (!cacheStatus.hasMembers()) {
         log.tracef("Not triggering rebalance for zero-members cache %s", cacheName);
         return;
      }

      if (!cacheStatus.hasJoiners() && isBalanced(cacheName, cacheStatus.getCacheTopology().getCurrentCH())) {
         log.tracef("Not triggering rebalance for cache %s, no joiners and the current consistent hash is already balanced",
                    cacheName);
         return;
      }

      if (cacheStatus.isRebalanceInProgress()) {
         log.tracef("Not triggering rebalance for cache %s, a rebalance is already in progress", cacheName);
         return;
      }

      synchronized (lock) {
         if (!isRebalancingEnabled()) {
            log.tracef("Rebalancing is disabled, queueing rebalance for cache %s", cacheName);
            cachesPendingRebalance.put(cacheName, cacheStatus.getCacheTopology().getCurrentCH());
            return;
         }
      }

      ConsistentHashChanges changes = getConsistentHashChanges(cacheName, cacheStatus.getCacheTopology().getCurrentCH());
      log.tracef("Triggering rebalance for cache %s", cacheName);
      clusterTopologyManager.triggerRebalance(cacheName, changes);
   }

   @Override
   public boolean isRebalancingEnabled() {
      synchronized (lock) {
         return cachesPendingRebalance == null;
      }
   }

   @Override
   public void setRebalancingEnabled(@Parameter(description = "enable?") boolean enabled) {
      Map<String, ConsistentHash> caches;
      synchronized (lock) {
         caches = cachesPendingRebalance;
         if (enabled) {
            if (log.isDebugEnabled()) {
               log.debugf("Rebalancing enabled");
            }
            cachesPendingRebalance = null;
         } else {
            if (log.isDebugEnabled()) {
               log.debugf("Rebalancing suspended");
            }
            cachesPendingRebalance = new HashMap<String, ConsistentHash>();
         }
      }

      if (enabled && caches != null) {
         if (log.isTraceEnabled()) {
            log.tracef("Rebalancing enabled, triggering rebalancing for caches %s", caches);
         }
         for (Map.Entry<String, ConsistentHash> pendingRebalance : caches.entrySet()) {
            String cacheName = pendingRebalance.getKey();
            try {

               clusterTopologyManager.triggerRebalance(cacheName,
                                                       getConsistentHashChanges(cacheName, pendingRebalance.getValue()));
            } catch (Exception e) {
               log.rebalanceStartError(cacheName, e);
            }
         }
      } else {
         log.debugf("Rebalancing suspended");
      }
   }

   public final void setNewSegmentMappings(String cacheName, ClusterObjectLookup segmentMappings) throws Exception {
      synchronized (consistentHashChangesMap) {
         ConsistentHashChanges consistentHashChanges = consistentHashChangesMap.get(cacheName);
         if (consistentHashChanges == null) {
            consistentHashChanges = new ConsistentHashChanges();
            consistentHashChangesMap.put(cacheName, consistentHashChanges);
         }
         consistentHashChanges.setNewMappings(segmentMappings);
      }
      clusterTopologyManager.updateCacheStatus(cacheName);
   }

   public final void setNewReplicationDegree(String cacheName, int replicationDegree) throws Exception {
      synchronized (consistentHashChangesMap) {
         ConsistentHashChanges consistentHashChanges = consistentHashChangesMap.get(cacheName);
         if (consistentHashChanges == null) {
            consistentHashChanges = new ConsistentHashChanges();
            consistentHashChangesMap.put(cacheName, consistentHashChanges);
         }
         consistentHashChanges.setNewReplicationDegree(replicationDegree);
      }
      clusterTopologyManager.updateCacheStatus(cacheName);
   }

   public final void setNewLCRDMappings(String cacheName, Map<String, Integer> transactionClassMap,
                                        Map<Integer, Float> clusterWeightMap) throws Exception {
      synchronized (consistentHashChangesMap) {
         ConsistentHashChanges consistentHashChanges = consistentHashChangesMap.get(cacheName);
         if (consistentHashChanges == null) {
            consistentHashChanges = new ConsistentHashChanges();
            consistentHashChangesMap.put(cacheName, consistentHashChanges);
         }
         consistentHashChanges.setLCRDMappings(transactionClassMap, clusterWeightMap);
      }
      clusterTopologyManager.updateCacheStatus(cacheName);
   }

   public boolean isBalanced(String cacheName, ConsistentHash ch) {
      int numSegments = ch.getNumSegments();
      for (int i = 0; i < numSegments; i++) {
         int actualNumOwners = Math.min(ch.getMembers().size(), ch.getNumOwners());
         if (ch.locateOwnersForSegment(i).size() != actualNumOwners) {
            return false;
         }
      }
      return getConsistentHashChanges(cacheName, ch) == null;
   }

   private ConsistentHashChanges getConsistentHashChanges(String cacheName, ConsistentHash consistentHash) {
      synchronized (consistentHashChangesMap) {
         ConsistentHashChanges consistentHashChanges = consistentHashChangesMap.get(cacheName);
         if (consistentHashChanges == null) {
            return null;
         }
         checkReplicationDegree(consistentHash, consistentHashChanges);
         if (consistentHash instanceof DataPlacementConsistentHash) {
            checkDataPlacementMappings((DataPlacementConsistentHash) consistentHash, consistentHashChanges);
            ConsistentHash original = ((DataPlacementConsistentHash) consistentHash).getConsistentHash();
            if (original instanceof LCRDConsistentHash) {
               checkLCRDMappings((LCRDConsistentHash) original, consistentHashChanges);
            }
         } else if (consistentHash instanceof LCRDConsistentHash) {
            checkLCRDMappings((LCRDConsistentHash) consistentHash, consistentHashChanges);
         }
         if (consistentHashChanges.hasChanges()) {
            return consistentHashChanges;
         }
         consistentHashChangesMap.remove(cacheName);
         return null;
      }
   }

   private void checkDataPlacementMappings(DataPlacementConsistentHash consistentHash,
                                           ConsistentHashChanges consistentHashChanges) {
      ClusterObjectLookup clusterObjectLookup = consistentHashChanges.getNewMappings();
      List list = consistentHash.getClusterObjectLookupList();
      if (clusterObjectLookup != null && list.size() == 1 && clusterObjectLookup.equals(list.get(0))) {
         //no changes in the mapping
         consistentHashChanges.setNewMappings(null);
      }
   }

   private void checkReplicationDegree(ConsistentHash consistentHash, ConsistentHashChanges consistentHashChanges) {
      int replicationDegree = consistentHashChanges.getNewReplicationDegree();
      if (replicationDegree != -1 && replicationDegree == consistentHash.getNumOwners()) {
         //no changes in replication degree
         consistentHashChanges.setNewReplicationDegree(-1);
      }
   }

   private void checkLCRDMappings(LCRDConsistentHash consistentHash, ConsistentHashChanges consistentHashChanges) {
      Map<String, Integer> transactionClassMap = consistentHashChanges.getTransactionClassMap();
      if (transactionClassMap == null) {
         consistentHashChanges.setLCRDMappings(null, null);
         return;
      }
      ExternalLCRDMappingEntry[] externalLCRDMappingEntries = consistentHash.getTransactionClassCluster();
      if (externalLCRDMappingEntries.length == 0) {
         //new mappings!
         return;
      }
      Map<Integer, Float> clusterWeightMap = consistentHashChanges.getClusterWeightMap();
      for (ExternalLCRDMappingEntry entry : externalLCRDMappingEntries) {
         LCRDCluster[] clusters = entry.getClusters();
         if (clusters.length == 0 || clusters.length > 1) {
            //the mappings has changed!
            return;
         }
         Integer clusterId = transactionClassMap.get(entry.getTransactionClass());
         if (clusterId == null || clusterId != clusters[0].getId()) {
            //the mappings has changed!
            return;
         }
         Float clusterWeight = clusterWeightMap.get(clusterId);
         if (clusterWeight == null || clusterWeight != clusters[0].getWeight()) {
            //the cluster weight has changed
            return;
         }
      }
      //the mappings are the same
      consistentHashChanges.setLCRDMappings(null, null);
   }
}
