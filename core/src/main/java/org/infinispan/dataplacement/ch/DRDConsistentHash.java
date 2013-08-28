package org.infinispan.dataplacement.ch;

import org.infinispan.commons.hash.Hash;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.Util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Pedro Ruivo
 * @since 5.2
 */
public class DRDConsistentHash implements ConsistentHash {

   private static final String[] EMPTY_TRANSACTION_CLASS_ARRAY = new String[0];
   private static final DRDCluster[] EMPTY_CLUSTER_ARRAY = new DRDCluster[0];
   private final ConsistentHash consistentHash;
   private final String[] sortedTransactionClasses;
   private final DRDCluster[] clusters;

   public DRDConsistentHash(ConsistentHash consistentHash) {
      this(consistentHash, EMPTY_TRANSACTION_CLASS_ARRAY, EMPTY_CLUSTER_ARRAY);
   }

   public DRDConsistentHash(ConsistentHash consistentHash, String[] sortedTransactionClasses, DRDCluster[] clusters) {
      this.consistentHash = consistentHash;
      this.sortedTransactionClasses = sortedTransactionClasses;
      this.clusters = clusters;
   }

   public DRDConsistentHash(DRDConsistentHash baseCH, ConsistentHash updatedConsistentHash) {
      this(updatedConsistentHash, baseCH.sortedTransactionClasses, baseCH.clusters);
   }

   @Override
   public int getNumOwners() {
      return consistentHash.getNumOwners();
   }

   @Override
   public Hash getHashFunction() {
      return consistentHash.getHashFunction();
   }

   @Override
   public int getNumSegments() {
      return consistentHash.getNumSegments();
   }

   @Override
   public List<Address> getMembers() {
      return consistentHash.getMembers();
   }

   @Override
   public Address locatePrimaryOwner(Object key) {
      Address[] addresses = lookupKey(key);
      return addresses == null ? consistentHash.locatePrimaryOwner(key) : addresses[0];
   }

   @Override
   public List<Address> locateOwners(Object key) {
      Address[] addresses = lookupKey(key);
      return addresses == null ? consistentHash.locateOwners(key) : Arrays.asList(addresses);
   }

   @Override
   public Set<Address> locateAllOwners(Collection<Object> keys) {
      Set<Address> addressSet = new HashSet<Address>();
      for (Object key : keys) {
         addressSet.addAll(locateOwners(key));
      }
      return addressSet;
   }

   @Override
   public boolean isKeyLocalToNode(Address nodeAddress, Object key) {
      return isKeyOwnByAddress(nodeAddress, key) || consistentHash.isKeyLocalToNode(nodeAddress, key);
   }

   @Override
   public int getSegment(Object key) {
      return consistentHash.getSegment(key);
   }

   @Override
   public List<Address> locateOwnersForSegment(int segmentId) {
      return consistentHash.locateOwnersForSegment(segmentId);
   }

   @Override
   public Address locatePrimaryOwnerForSegment(int segmentId) {
      return consistentHash.locatePrimaryOwnerForSegment(segmentId);
   }

   @Override
   public Set<Integer> getSegmentsForOwner(Address owner) {
      return consistentHash.getSegmentsForOwner(owner);
   }

   @Override
   public String getRoutingTableAsString() {
      return consistentHash.getRoutingTableAsString();
   }

   public ConsistentHash getConsistentHash() {
      return consistentHash;
   }

   public Map<String, DRDCluster> getTransactionClassCluster() {
      Map<String, DRDCluster> map = new HashMap<String, DRDCluster>(sortedTransactionClasses.length);
      for (int i = 0; i < sortedTransactionClasses.length; ++i) {
         map.put(sortedTransactionClasses[i], clusters[i]);
      }
      return map;
   }

   public boolean hasMappings() {
      return sortedTransactionClasses.length != 0;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      DRDConsistentHash that = (DRDConsistentHash) o;

      return Arrays.equals(clusters, that.clusters) &&
            consistentHash.equals(that.consistentHash) &&
            Arrays.equals(sortedTransactionClasses, that.sortedTransactionClasses);

   }

   @Override
   public int hashCode() {
      int result = consistentHash.hashCode();
      result = 31 * result + Arrays.hashCode(sortedTransactionClasses);
      result = 31 * result + Arrays.hashCode(clusters);
      return result;
   }

   private DRDCluster clusterOf(String transactionClass) {
      if (sortedTransactionClasses.length == 0 || clusters.length == 0) {
         return null;
      }
      int idx = Arrays.binarySearch(sortedTransactionClasses, transactionClass);
      return idx < 0 || idx >= clusters.length ? null : clusters[idx];
   }

   private Address[] lookupKey(Object key) {
      if (key instanceof String) {
         DRDCluster cluster = clusterOf((String) key);
         return cluster == null ? null : cluster.getMembers();
      }
      return null;
   }

   private boolean isKeyOwnByAddress(Address address, Object key) {
      Address[] addresses = lookupKey(key);
      return addresses != null && Arrays.asList(addresses).contains(address);
   }

   public static class Externalizer extends AbstractExternalizer<DRDConsistentHash> {

      @Override
      public Integer getId() {
         return Ids.DRD_CH;
      }

      @Override
      public Set<Class<? extends DRDConsistentHash>> getTypeClasses() {
         return Util.<Class<? extends DRDConsistentHash>>asSet(DRDConsistentHash.class);
      }

      @Override
      public void writeObject(ObjectOutput output, DRDConsistentHash object) throws IOException {
         output.writeObject(object.consistentHash);
         output.writeInt(object.sortedTransactionClasses.length);
         output.writeInt(object.clusters.length);
         for (String s : object.sortedTransactionClasses) {
            output.writeUTF(s);
         }
         for (DRDCluster cluster : object.clusters) {
            cluster.writeTo(output);
         }
      }

      @Override
      public DRDConsistentHash readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         ConsistentHash consistentHash = (ConsistentHash) input.readObject();
         int size = input.readInt();
         final String[] txClasses = size == 0 ? EMPTY_TRANSACTION_CLASS_ARRAY : new String[size];
         size = input.readInt();
         final DRDCluster[] clusters = size == 0 ? EMPTY_CLUSTER_ARRAY : new DRDCluster[size];
         for (int i = 0; i < txClasses.length; ++i) {
            txClasses[i] = input.readUTF();
         }
         for (int i = 0; i < clusters.length; ++i) {
            clusters[i] = DRDCluster.readFrom(input);
         }
         return new DRDConsistentHash(consistentHash, txClasses, clusters);
      }
   }

}
