package org.infinispan.dataplacement.ch;

import org.infinispan.commons.hash.Hash;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.ConsistentHashFactory;
import org.infinispan.distribution.ch.DefaultConsistentHashFactory;
import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.Util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.infinispan.dataplacement.ch.LCRDClusterUtil.calculateClustersWeight;
import static org.infinispan.dataplacement.ch.LCRDClusterUtil.createClusterMembers;

/**
 * @author Pedro Ruivo
 * @since 5.2
 */
public class LCRDConsistentHashFactory implements ConsistentHashFactory<LCRDConsistentHash> {

   private final ConsistentHashFactory consistentHashFactory;

   public LCRDConsistentHashFactory() {
      this(new DefaultConsistentHashFactory());
   }

   private LCRDConsistentHashFactory(ConsistentHashFactory consistentHashFactory) {
      this.consistentHashFactory = consistentHashFactory;
   }

   @Override
   public LCRDConsistentHash create(Hash hashFunction, int numOwners, int numSegments, List<Address> members) {
      return new LCRDConsistentHash(consistentHashFactory.create(hashFunction, numOwners, numSegments, members));
   }

   @Override
   public LCRDConsistentHash updateMembers(LCRDConsistentHash baseCH, List<Address> newMembers) {
      ConsistentHash updatedConsistentHash = consistentHashFactory.updateMembers(baseCH.getConsistentHash(), newMembers);
      LCRDConsistentHash updatedLCRDConsistentHash;
      if (!baseCH.hasMappings()) {
         updatedLCRDConsistentHash = new LCRDConsistentHash(updatedConsistentHash);
      } else {
         updatedLCRDConsistentHash = removeLeavers(baseCH, updatedConsistentHash, newMembers);
      }
      return baseCH.equals(updatedConsistentHash) ? baseCH : updatedLCRDConsistentHash;
   }

   @Override
   public LCRDConsistentHash rebalance(LCRDConsistentHash baseCH, Object customData) {
      ConsistentHash rebalancedConsistentHash = consistentHashFactory.rebalance(baseCH.getConsistentHash(), customData);
      LCRDConsistentHash rebalancedLCRDConsistentHash;
      if (customDataHasNewMappings(customData)) {
         ConsistentHashChanges consistentHashChanges = (ConsistentHashChanges) customData;
         Map<String, Integer> transactionClassMap = consistentHashChanges.getTransactionClassMap();
         Map<Integer, Float> clusterWeightMap = consistentHashChanges.getClusterWeightMap();
         rebalancedLCRDConsistentHash = newRebalancedMappings(rebalancedConsistentHash, transactionClassMap,
                                                              clusterWeightMap);
      } else if (baseCH.hasMappings()) {
         rebalancedLCRDConsistentHash = rebalanceMappings(baseCH, rebalancedConsistentHash);
      } else {
         rebalancedLCRDConsistentHash = new LCRDConsistentHash(baseCH, rebalancedConsistentHash);
      }
      return baseCH.equals(rebalancedLCRDConsistentHash) ? baseCH : rebalancedLCRDConsistentHash;
   }

   @Override
   public LCRDConsistentHash union(LCRDConsistentHash ch1, LCRDConsistentHash ch2) {
      final ConsistentHash unionConsistentHash = consistentHashFactory.union(ch1.getConsistentHash(), ch2.getConsistentHash());
      LCRDConsistentHash unionLCRDConsistentHash;
      if (ch1.hasMappings() && ch2.hasMappings()) {
         unionLCRDConsistentHash = unionMappings(ch1, ch2, unionConsistentHash);
      } else if (ch1.hasMappings()) {
         unionLCRDConsistentHash = new LCRDConsistentHash(ch1, unionConsistentHash);
      } else if (ch2.hasMappings()) {
         unionLCRDConsistentHash = new LCRDConsistentHash(ch2, unionConsistentHash);
      } else {
         unionLCRDConsistentHash = new LCRDConsistentHash(unionConsistentHash);
      }
      return unionLCRDConsistentHash;
   }

   private LCRDConsistentHash unionMappings(LCRDConsistentHash ch1, LCRDConsistentHash ch2, ConsistentHash unionConsistentHash) {

      return null;  //To change body of created methods use File | Settings | File Templates.
   }

   private LCRDConsistentHash newRebalancedMappings(ConsistentHash rebalancedConsistentHash,
                                                    Map<String, Integer> transactionClassMap,
                                                    Map<Integer, Float> clusterWeightMap) {
      final float[] clusterWeights = calculateClustersWeight(clusterWeightMap);
      final List<Address> members = rebalancedConsistentHash.getMembers();
      final int numOwners = rebalancedConsistentHash.getNumOwners();
      final Map<String, LCRDCluster> clusterMap = new HashMap<String, LCRDCluster>(transactionClassMap.size());
      for (Map.Entry<String, Integer> entry : transactionClassMap.entrySet()) {
         clusterMap.put(entry.getKey(), createDRDCluster(entry.getValue(), clusterWeights, members, numOwners));
      }
      return createDRDConsistentHash(rebalancedConsistentHash, clusterMap);
   }

   private LCRDConsistentHash rebalanceMappings(LCRDConsistentHash baseCH, ConsistentHash rebalancedConsistentHash) {
      final Map<String, LCRDCluster> clusterMap = baseCH.getTransactionClassCluster();
      final float[] clusterWeights = calculateClustersWeight(clusterMap.values());
      final List<Address> members = rebalancedConsistentHash.getMembers();
      final int numOwners = rebalancedConsistentHash.getNumOwners();
      for (Map.Entry<String, LCRDCluster> entry : clusterMap.entrySet()) {
         entry.setValue(createDRDCluster(entry.getValue(), clusterWeights, members, numOwners));
      }
      return createDRDConsistentHash(rebalancedConsistentHash, clusterMap);
   }

   private boolean customDataHasNewMappings(Object customData) {
      return customData != null && customData instanceof ConsistentHashChanges &&
            ((ConsistentHashChanges) customData).getTransactionClassMap() != null;
   }

   private LCRDConsistentHash removeLeavers(LCRDConsistentHash baseCH, ConsistentHash updatedConsistentHash,
                                            List<Address> newMembers) {
      boolean changed = false;
      final Map<String, LCRDCluster> clusterMap = baseCH.getTransactionClassCluster();
      final float[] clusterWeights = calculateClustersWeight(clusterMap.values());
      for (Map.Entry<String, LCRDCluster> entry : clusterMap.entrySet()) {
         LCRDCluster cluster = entry.getValue();
         Set<Address> clusterMembers = new HashSet<Address>(Arrays.asList(cluster.getMembers()));
         clusterMembers.retainAll(newMembers);
         if (clusterMembers.isEmpty()) {
            changed = true;
            entry.setValue(createDRDCluster(cluster, clusterWeights, newMembers, baseCH.getNumOwners()));
         }
      }
      return changed ? createDRDConsistentHash(updatedConsistentHash, clusterMap) :
            new LCRDConsistentHash(baseCH, updatedConsistentHash);
   }

   private LCRDConsistentHash createDRDConsistentHash(ConsistentHash consistentHash, Map<String, LCRDCluster> clusterMap) {
      if (clusterMap == null || clusterMap.isEmpty()) {
         return new LCRDConsistentHash(consistentHash);
      }
      final String[] transactionClasses = new String[clusterMap.size()];
      final LCRDCluster[] clusters = new LCRDCluster[clusterMap.size()];
      clusterMap.keySet().toArray(transactionClasses);
      Arrays.sort(transactionClasses);
      for (int i = 0; i < transactionClasses.length; ++i) {
         clusters[i] = clusterMap.get(transactionClasses[i]);
      }
      return new LCRDConsistentHash(consistentHash, transactionClasses, clusters);
   }

   private LCRDCluster createDRDCluster(int id, float[] clusterWeights, List<Address> members, int numOwners) {
      return new LCRDCluster(id, clusterWeights[id], createClusterMembers(id, clusterWeights, members, numOwners));
   }

   private LCRDCluster createDRDCluster(LCRDCluster base, float[] clusterWeights, List<Address> members, int numOwners) {
      return createDRDCluster(base.getId(), clusterWeights, members, numOwners);
   }

   //private LCRDCluster

   public static class Externalizer extends AbstractExternalizer<LCRDConsistentHashFactory> {

      @Override
      public Integer getId() {
         return Ids.LCRD_CH_FACTORY;
      }

      @Override
      public Set<Class<? extends LCRDConsistentHashFactory>> getTypeClasses() {
         return Util.<Class<? extends LCRDConsistentHashFactory>>asSet(LCRDConsistentHashFactory.class);
      }

      @Override
      public void writeObject(ObjectOutput output, LCRDConsistentHashFactory object) throws IOException {
         output.writeObject(object.consistentHashFactory);
      }

      @Override
      public LCRDConsistentHashFactory readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new LCRDConsistentHashFactory((ConsistentHashFactory) input.readObject());
      }
   }
}
