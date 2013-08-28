package org.infinispan.dataplacement.ch;

import org.infinispan.commons.hash.Hash;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.ConsistentHashFactory;
import org.infinispan.distribution.ch.DefaultConsistentHashFactory;
import org.infinispan.remoting.transport.Address;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.infinispan.dataplacement.ch.DRDClusterUtil.calculateClustersWeight;
import static org.infinispan.dataplacement.ch.DRDClusterUtil.createClusterMembers;

/**
 * @author Pedro Ruivo
 * @since 5.2
 */
public class DRDConsistentHashFactory implements ConsistentHashFactory<DRDConsistentHash> {

   private final ConsistentHashFactory consistentHashFactory;

   public DRDConsistentHashFactory() {
      consistentHashFactory = new DefaultConsistentHashFactory();
   }

   @Override
   public DRDConsistentHash create(Hash hashFunction, int numOwners, int numSegments, List<Address> members) {
      return new DRDConsistentHash(consistentHashFactory.create(hashFunction, numOwners, numSegments, members));
   }

   @Override
   public DRDConsistentHash updateMembers(DRDConsistentHash baseCH, List<Address> newMembers) {
      ConsistentHash updatedConsistentHash = consistentHashFactory.updateMembers(baseCH.getConsistentHash(), newMembers);
      DRDConsistentHash updatedDRDConsistentHash;
      if (!baseCH.hasMappings()) {
         updatedDRDConsistentHash = new DRDConsistentHash(updatedConsistentHash);
      } else {
         updatedDRDConsistentHash = removeLeavers(baseCH, updatedConsistentHash, newMembers);
      }
      return baseCH.equals(updatedConsistentHash) ? baseCH : updatedDRDConsistentHash;
   }

   @Override
   public DRDConsistentHash rebalance(DRDConsistentHash baseCH, Object customData) {
      ConsistentHash rebalancedConsistentHash = consistentHashFactory.rebalance(baseCH.getConsistentHash(), customData);
      DRDConsistentHash rebalancedDRDConsistentHash;
      if (customDataHasNewMappings(customData)) {
         ConsistentHashChanges consistentHashChanges = (ConsistentHashChanges) customData;
         Map<String, Integer> transactionClassMap = consistentHashChanges.getTransactionClassMap();
         Map<Integer, Float> clusterWeightMap = consistentHashChanges.getClusterWeightMap();
         rebalancedDRDConsistentHash = newRebalancedMappings(rebalancedConsistentHash, transactionClassMap,
                                                             clusterWeightMap);
      } else if (baseCH.hasMappings()) {
         rebalancedDRDConsistentHash = rebalanceMappings(baseCH, rebalancedConsistentHash);
      } else {
         rebalancedDRDConsistentHash = new DRDConsistentHash(baseCH, rebalancedConsistentHash);
      }
      return baseCH.equals(rebalancedDRDConsistentHash) ? baseCH : rebalancedDRDConsistentHash;
   }

   @Override
   public DRDConsistentHash union(DRDConsistentHash ch1, DRDConsistentHash ch2) {
      final ConsistentHash unionConsistentHash = consistentHashFactory.union(ch1.getConsistentHash(), ch2.getConsistentHash());
      DRDConsistentHash unionDRDConsistentHash;
      if (ch1.hasMappings() && ch2.hasMappings()) {
         unionDRDConsistentHash = unionMappings(ch1, ch2, unionConsistentHash);
      } else if (ch1.hasMappings()) {
         unionDRDConsistentHash = new DRDConsistentHash(ch1, unionConsistentHash);
      } else if (ch2.hasMappings()) {
         unionDRDConsistentHash = new DRDConsistentHash(ch2, unionConsistentHash);
      } else {
         unionDRDConsistentHash = new DRDConsistentHash(unionConsistentHash);
      }
      return unionDRDConsistentHash;
   }

   private DRDConsistentHash unionMappings(DRDConsistentHash ch1, DRDConsistentHash ch2, ConsistentHash unionConsistentHash) {

      return null;  //To change body of created methods use File | Settings | File Templates.
   }

   private DRDConsistentHash newRebalancedMappings(ConsistentHash rebalancedConsistentHash,
                                                   Map<String, Integer> transactionClassMap,
                                                   Map<Integer, Float> clusterWeightMap) {
      final float[] clusterWeights = calculateClustersWeight(clusterWeightMap);
      final List<Address> members = rebalancedConsistentHash.getMembers();
      final int numOwners = rebalancedConsistentHash.getNumOwners();
      final Map<String, DRDCluster> clusterMap = new HashMap<String, DRDCluster>(transactionClassMap.size());
      for (Map.Entry<String, Integer> entry : transactionClassMap.entrySet()) {
         clusterMap.put(entry.getKey(), createDRDCluster(entry.getValue(), clusterWeights, members, numOwners));
      }
      return createDRDConsistentHash(rebalancedConsistentHash, clusterMap);
   }

   private DRDConsistentHash rebalanceMappings(DRDConsistentHash baseCH, ConsistentHash rebalancedConsistentHash) {
      final Map<String, DRDCluster> clusterMap = baseCH.getTransactionClassCluster();
      final float[] clusterWeights = calculateClustersWeight(clusterMap.values());
      final List<Address> members = rebalancedConsistentHash.getMembers();
      final int numOwners = rebalancedConsistentHash.getNumOwners();
      for (Map.Entry<String, DRDCluster> entry : clusterMap.entrySet()) {
         entry.setValue(createDRDCluster(entry.getValue(), clusterWeights, members, numOwners));
      }
      return createDRDConsistentHash(rebalancedConsistentHash, clusterMap);
   }

   private boolean customDataHasNewMappings(Object customData) {
      return customData != null && customData instanceof ConsistentHashChanges &&
            ((ConsistentHashChanges) customData).getTransactionClassMap() != null;
   }

   private DRDConsistentHash removeLeavers(DRDConsistentHash baseCH, ConsistentHash updatedConsistentHash,
                                           List<Address> newMembers) {
      boolean changed = false;
      final Map<String, DRDCluster> clusterMap = baseCH.getTransactionClassCluster();
      final float[] clusterWeights = calculateClustersWeight(clusterMap.values());
      for (Map.Entry<String, DRDCluster> entry : clusterMap.entrySet()) {
         DRDCluster cluster = entry.getValue();
         Set<Address> clusterMembers = new HashSet<Address>(Arrays.asList(cluster.getMembers()));
         clusterMembers.retainAll(newMembers);
         if (clusterMembers.isEmpty()) {
            changed = true;
            entry.setValue(createDRDCluster(cluster, clusterWeights, newMembers, baseCH.getNumOwners()));
         }
      }
      return changed ? createDRDConsistentHash(updatedConsistentHash, clusterMap) :
            new DRDConsistentHash(baseCH, updatedConsistentHash);
   }

   private DRDConsistentHash createDRDConsistentHash(ConsistentHash consistentHash, Map<String, DRDCluster> clusterMap) {
      if (clusterMap == null || clusterMap.isEmpty()) {
         return new DRDConsistentHash(consistentHash);
      }
      final String[] transactionClasses = new String[clusterMap.size()];
      final DRDCluster[] clusters = new DRDCluster[clusterMap.size()];
      clusterMap.keySet().toArray(transactionClasses);
      Arrays.sort(transactionClasses);
      for (int i = 0; i < transactionClasses.length; ++i) {
         clusters[i] = clusterMap.get(transactionClasses[i]);
      }
      return new DRDConsistentHash(consistentHash, transactionClasses, clusters);
   }

   private DRDCluster createDRDCluster(int id, float[] clusterWeights, List<Address> members, int numOwners) {
      return new DRDCluster(id, clusterWeights[id], createClusterMembers(id, clusterWeights, members, numOwners));
   }

   private DRDCluster createDRDCluster(DRDCluster base, float[] clusterWeights, List<Address> members, int numOwners) {
      return createDRDCluster(base.getId(), clusterWeights, members, numOwners);
   }

   //private DRDCluster
}
