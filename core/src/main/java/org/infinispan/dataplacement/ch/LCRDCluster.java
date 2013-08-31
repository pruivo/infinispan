package org.infinispan.dataplacement.ch;

import org.infinispan.remoting.transport.Address;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.List;

import static org.infinispan.dataplacement.ch.LCRDClusterUtil.createClusterMembers;

/**
 * @author Pedro Ruivo
 * @since 5.2
 */
public class LCRDCluster {
   private final int id;
   private final float weight;
   private final Address[] members;

   public LCRDCluster(int id, float weight, Address[] members) {
      this.id = id;
      this.weight = weight;
      this.members = members;
   }

   public int getId() {
      return id;
   }

   public float getWeight() {
      return weight;
   }

   public Address[] getMembers() {
      return members;
   }

   public final void writeTo(ObjectOutput output) throws IOException {
      output.writeInt(id);
      output.writeFloat(weight);
      output.writeInt(members.length);
      for (Address address : members) {
         output.writeObject(address);
      }
   }

   public static LCRDCluster readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      final int id = input.readInt();
      final float weight = input.readFloat();
      final Address[] members = new Address[input.readInt()];
      for (int i = 0; i < members.length; ++i) {
         members[i] = (Address) input.readObject();
      }
      return new LCRDCluster(id, weight, members);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      LCRDCluster that = (LCRDCluster) o;

      return id == that.id && Float.compare(that.weight, weight) == 0 && Arrays.equals(members, that.members);

   }

   @Override
   public int hashCode() {
      int result = id;
      result = 31 * result + (weight != +0.0f ? Float.floatToIntBits(weight) : 0);
      result = 31 * result + Arrays.hashCode(members);
      return result;
   }

   public static LCRDCluster createLCRDCluster(int id, float[] clusterWeights, List<Address> members, int numOwners) {
      return new LCRDCluster(id, clusterWeights[id], createClusterMembers(id, clusterWeights, members, numOwners));
   }

   public static LCRDCluster createLCRDCluster(LCRDCluster base, float[] clusterWeights, List<Address> members, int numOwners) {
      return createLCRDCluster(base.getId(), clusterWeights, members, numOwners);
   }

   public static LCRDCluster updateClusterMembers(LCRDCluster base, List<Address> members) {
      return new LCRDCluster(base.getId(), base.getWeight(), members.toArray(new Address[members.size()]));
   }
}
