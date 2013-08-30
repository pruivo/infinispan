package org.infinispan.dataplacement.ch;

import org.infinispan.remoting.transport.Address;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Pedro Ruivo
 * @since 5.2
 */
public class LCRDClusterUtil {

   public static Address[] createClusterMembers(int id, float[] clusterWeight, List<Address> members, int numOwners) {
      Address[] clusterMembers = new Address[clusterSize(clusterWeight, id, members.size(), numOwners)];
      int startIdx = startIndex(clusterWeight, id, members.size());
      for (int i = 0; i < clusterMembers.length; ++i) {
         clusterMembers[i] = members.get(startIdx++ % members.size());
      }
      return clusterMembers;
   }

   public static float[] calculateClustersWeight(Collection<LCRDCluster> clusters) {
      Map<Integer, Float> clusterWeightMap = new HashMap<Integer, Float>();
      for (LCRDCluster cluster : clusters) {
         clusterWeightMap.put(cluster.getId(), cluster.getWeight());
      }
      return calculateClustersWeight(clusterWeightMap);
   }

   public static float[] calculateClustersWeight(Map<Integer, Float> clusterWeightMap) {
      final float[] clusterWeightArray = new float[clusterWeightMap.size()];
      for (Map.Entry<Integer, Float> entry : clusterWeightMap.entrySet()) {
         clusterWeightArray[entry.getKey()] = entry.getValue();
      }
      return clusterWeightArray;
   }

   public static int startIndex(float[] clusterWeight, int id, int size) {
      if (clusterWeight.length >= size) {
         return id % size;
      }
      int index = 0;
      for (int i = 0; i < id; ++i) {
         index += clusterSize(clusterWeight, i, size, 1);
      }
      return index;
   }

   public static int clusterSize(float[] clusterWeight, int id, int size, int numOwners) {
      if (numOwners >= size) {
         //full replication
         return size;
      }
      return (int) Math.max(clusterWeight[id] * size, numOwners);
   }

}
