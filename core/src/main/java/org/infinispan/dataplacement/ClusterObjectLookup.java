package org.infinispan.dataplacement;

import org.infinispan.commons.hash.Hash;
import org.infinispan.dataplacement.lookup.ObjectLookup;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.remoting.transport.Address;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author Pedro Ruivo
 * @since 5.2
 */
public class ClusterObjectLookup {

   private final ObjectLookup[] objectLookups;
   private final ClusterSnapshot clusterSnapshot;

   public ClusterObjectLookup(ObjectLookup[] objectLookups, ClusterSnapshot clusterSnapshot) {
      this.objectLookups = objectLookups;
      this.clusterSnapshot = clusterSnapshot;
   }

   public static void write(ObjectOutput output, ClusterObjectLookup object) throws IOException {
      if (object == null || object.objectLookups == null || object.objectLookups.length == 0) {
         output.writeInt(0);
      } else {
         output.writeInt(object.objectLookups.length);
         for (ObjectLookup objectLookup : object.objectLookups) {
            output.writeObject(objectLookup);
         }
         List<Address> members = object.clusterSnapshot.getMembers();
         output.writeInt(members.size());
         for (Address address : members) {
            output.writeObject(address);
         }
      }
   }

   public static ClusterObjectLookup read(ObjectInput input, Hash hash) throws IOException, ClassNotFoundException {
      int size = input.readInt();
      if (size == 0) {
         return null;
      }
      ObjectLookup[] objectLookups = new ObjectLookup[size];
      for (int i = 0; i < size; ++i) {
         objectLookups[i] = (ObjectLookup) input.readObject();
      }
      size = input.readInt();
      Address[] members = new Address[size];
      for (int i = 0; i < size; ++i) {
         members[i] = (Address) input.readObject();
      }
      return new ClusterObjectLookup(objectLookups, new ClusterSnapshot(members, hash));
   }

   public final List<Address> getNewOwnersForKey(Object key, ConsistentHash consistentHash, int numberOfOwners) {
      if (objectLookups == null || objectLookups.length == 0 || objectLookups[consistentHash.getSegment(key)] == null) {
         return null;
      }
      List<Integer> newOwnersIndexes = objectLookups[consistentHash.getSegment(key)].query(key);
      if (newOwnersIndexes == null || newOwnersIndexes.isEmpty()) {
         return null;
      }
      List<Address> newOwners = new ArrayList<Address>(Math.min(numberOfOwners, newOwnersIndexes.size()));
      for (Iterator<Integer> iterator = newOwnersIndexes.iterator(); iterator.hasNext() && newOwners.size() < numberOfOwners; ) {
         Address owner = clusterSnapshot.get(iterator.next());
         if (owner != null && consistentHash.getMembers().contains(owner)) {
            newOwners.add(owner);
         }
      }
      return newOwners;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      ClusterObjectLookup that = (ClusterObjectLookup) o;

      return !(clusterSnapshot != null ? !clusterSnapshot.equals(that.clusterSnapshot) : that.clusterSnapshot != null) &&
            Arrays.equals(objectLookups, that.objectLookups);

   }

   @Override
   public int hashCode() {
      int result = objectLookups != null ? Arrays.hashCode(objectLookups) : 0;
      result = 31 * result + (clusterSnapshot != null ? clusterSnapshot.hashCode() : 0);
      return result;
   }

}
