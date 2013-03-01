package org.infinispan.dataplacement.stats;

import org.infinispan.dataplacement.OwnersInfo;
import org.infinispan.dataplacement.c50.C50MLObjectLookup;
import org.infinispan.dataplacement.c50.lookup.BloomFilter;
import org.infinispan.dataplacement.c50.tree.DecisionTree;
import org.infinispan.dataplacement.lookup.ObjectLookup;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Task that checks the number of keys that was move to a wrong node, the average query duration and the size of the
 * object lookup
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class ObjectLookupTask implements Runnable {

   private static final Log log = LogFactory.getLog(ObjectLookupTask.class);
   private final ObjectLookup objectLookup;
   private final Map<Object, OwnersInfo> ownersInfoMap;
   private final Stats stats;
   private final IncrementableLong[] phaseDurations;

   public ObjectLookupTask(Map<Object, OwnersInfo> ownersInfoMap, ObjectLookup objectLookup, Stats stats) {
      this.ownersInfoMap = ownersInfoMap;
      this.objectLookup = objectLookup;
      this.stats = stats;
      this.phaseDurations = stats.createQueryPhaseDurationsArray();
   }

   @Override
   public void run() {
      int errors = 0;
      for (Map.Entry<Object, OwnersInfo> entry : ownersInfoMap.entrySet()) {
         Set<Integer> expectedOwners = new TreeSet<Integer>(entry.getValue().getNewOwnersIndexes());
         Collection<Integer> result = objectLookup.queryWithProfiling(entry.getKey(), phaseDurations);
         Set<Integer> ownersQuery = new TreeSet<Integer>(result);

         errors += expectedOwners.containsAll(ownersQuery) ? 0 : 1;
      }
      stats.wrongOwnersErrors(errors);
      stats.totalKeysMoved(ownersInfoMap.size());
      stats.queryDuration(phaseDurations);
      stats.objectLookupSize(serializedSize(objectLookup));
      if (objectLookup instanceof C50MLObjectLookup) {
         C50MLObjectLookup c50MLObjectLookup = (C50MLObjectLookup) objectLookup;
         BloomFilter bloomFilter = c50MLObjectLookup.getBloomFilter();
         stats.setBloomFilterSize(serializedSize(bloomFilter));
         DecisionTree[] trees = c50MLObjectLookup.getDecisionTreeArray();
         if (trees.length == 1) {
            stats.setMachineLearner1(serializedSize(trees[0]));
         } else if (trees.length > 1) {
            stats.setMachineLearner1(serializedSize(trees[0]));
            stats.setMachineLearner2(serializedSize(trees[1]));
         }
      }
   }

   private int serializedSize(Object object) {
      int size = 0;
      try {
         ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
         ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
         objectOutputStream.writeObject(object);
         objectOutputStream.flush();

         size = byteArrayOutputStream.toByteArray().length;
         objectOutputStream.close();
         byteArrayOutputStream.close();
      } catch (IOException e) {
         log.warnf(e, "Error calculating object size of %s", object);
      }
      return size;
   }
}
