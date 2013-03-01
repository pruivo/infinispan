package org.infinispan.dataplacement.hm;

import org.infinispan.dataplacement.SegmentMapping;
import org.infinispan.dataplacement.lookup.ObjectLookup;
import org.infinispan.dataplacement.stats.IncrementableLong;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * the object lookup implementation for the Hash Map technique
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class HashMapObjectLookup implements ObjectLookup {

   private final Map<Object, List<Integer>> lookup;

   public HashMapObjectLookup(Iterator<SegmentMapping.KeyOwners> iterator) {
      lookup = new HashMap<Object, List<Integer>>();

      while (iterator.hasNext()) {
         SegmentMapping.KeyOwners owners = iterator.next();
         List<Integer> list = new LinkedList<Integer>();
         for (int index : owners.getOwnerIndexes()) {
            list.add(index);
         }
         lookup.put(owners.getKey(), list);
      }
   }

   @Override
   public List<Integer> query(Object key) {
      return lookup.get(key);
   }

   @Override
   public List<Integer> queryWithProfiling(Object key, IncrementableLong[] phaseDurations) {
      long start = System.nanoTime();
      List<Integer> result = lookup.get(key);
      long end = System.nanoTime();

      if (phaseDurations.length == 1) {
         phaseDurations[0].add(end - start);
      }

      return result;
   }
}
