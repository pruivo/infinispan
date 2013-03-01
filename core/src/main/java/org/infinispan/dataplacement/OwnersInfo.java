package org.infinispan.dataplacement;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Maintains information about the new owners and their number of accesses
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class OwnersInfo implements Serializable {
   private final ArrayList<Integer> ownersIndexes;
   private final ArrayList<Long> ownersAccesses;

   public OwnersInfo(int size) {
      ownersIndexes = new ArrayList<Integer>(size);
      ownersAccesses = new ArrayList<Long>(size);
   }

   public void add(int ownerIndex, long numberOfAccesses) {
      ownersIndexes.add(ownerIndex);
      ownersAccesses.add(numberOfAccesses);
   }

   public void calculateNewOwner(int requestIdx, long numberOfAccesses) {
      if (ownersIndexes.contains(requestIdx)) {
         //already there
         return;
      }

      int toReplaceIndex = -1;
      long minAccesses = numberOfAccesses;

      for (int index = 0; index < ownersAccesses.size(); ++index) {
         if (ownersAccesses.get(index) < minAccesses) {
            minAccesses = ownersAccesses.get(index);
            toReplaceIndex = index;
         }
      }

      if (toReplaceIndex != -1) {
         ownersIndexes.set(toReplaceIndex, requestIdx);
         ownersAccesses.set(toReplaceIndex, numberOfAccesses);
      }
   }

   public int getOwner(int index) {
      if (index > ownersIndexes.size()) {
         return -1;
      }
      return ownersIndexes.get(index);
   }

   public int getReplicationCount() {
      return ownersIndexes.size();
   }

   public List<Integer> getNewOwnersIndexes() {
      return new LinkedList<Integer>(ownersIndexes);
   }

   @Override
   public String toString() {
      return "OwnersInfo{" +
            "ownersIndexes=" + ownersIndexes +
            ", ownersAccesses=" + ownersAccesses +
            '}';
   }
}
