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
import java.util.ArrayList;
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
public class LCRDConsistentHash implements ConsistentHash {

   private static final MappingEntry[] EMPTY_MAPPING_ENTRY_ARRAY = new MappingEntry[0];
   private static final ExternalLCRDMappingEntry[] EMPTY_EXTERNAL_MAPPING_ENTRY_ARRAY = new ExternalLCRDMappingEntry[0];
   private final ConsistentHash consistentHash;
   private final MappingEntry[] mappingEntries;

   public LCRDConsistentHash(ConsistentHash consistentHash) {
      this(consistentHash, EMPTY_MAPPING_ENTRY_ARRAY);
   }

   public LCRDConsistentHash(ConsistentHash consistentHash, String[] sortedTransactionClasses, LCRDCluster[] clusters) {
      this.consistentHash = consistentHash;
      if (isEmpty(sortedTransactionClasses, clusters)) {
         this.mappingEntries = EMPTY_MAPPING_ENTRY_ARRAY;
      } else {
         this.mappingEntries = new MappingEntry[1];
         this.mappingEntries[0] = new MappingEntry(sortedTransactionClasses, clusters);
      }
   }

   public LCRDConsistentHash(LCRDConsistentHash baseCH, ConsistentHash updatedConsistentHash) {
      this(updatedConsistentHash, baseCH.mappingEntries);
   }

   public LCRDConsistentHash(ConsistentHash consistentHash, ExternalLCRDMappingEntry[] externalLCRDMappingEntries) {
      this.consistentHash = consistentHash;
      if (externalLCRDMappingEntries == null || externalLCRDMappingEntries.length == 0) {
         this.mappingEntries = EMPTY_MAPPING_ENTRY_ARRAY;
      } else {
         this.mappingEntries = new MappingEntry[externalLCRDMappingEntries[0].getClusters().length];
         for (int i = 0; i < mappingEntries.length; ++i) {
            Set<String> txClass = new HashSet<String>(externalLCRDMappingEntries.length);
            Map<Integer, LCRDCluster> clusterMap = new HashMap<Integer, LCRDCluster>();
            for (ExternalLCRDMappingEntry entry : externalLCRDMappingEntries) {
               if (entry.getClusters()[i] != null) {
                  txClass.add(entry.getTransactionClass());
                  clusterMap.put(entry.getClusters()[i].getId(), entry.getClusters()[i]);
               }
            }
            final String[] txClassArray = new String[txClass.size()];
            txClass.toArray(txClassArray);
            final LCRDCluster[] clusterArray = new LCRDCluster[clusterMap.size()];
            for (LCRDCluster cluster : clusterMap.values()) {
               clusterArray[cluster.getId()] = cluster;
            }
            mappingEntries[i] = new MappingEntry(txClassArray, clusterArray);
         }
      }
   }

   private LCRDConsistentHash(ConsistentHash consistentHash, MappingEntry[] mappingEntries) {
      this.consistentHash = consistentHash;
      this.mappingEntries = mappingEntries;
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

   public ExternalLCRDMappingEntry[] getTransactionClassCluster() {
      if (mappingEntries.length == 0) {
         return EMPTY_EXTERNAL_MAPPING_ENTRY_ARRAY;
      }
      Map<String, ExternalLCRDMappingEntry> map = new HashMap<String, ExternalLCRDMappingEntry>();
      for (int i = 0; i < mappingEntries.length; ++i) {
         mappingEntries[i].addToMap(map, i, mappingEntries.length);
      }
      return map.values().toArray(new ExternalLCRDMappingEntry[map.size()]);
   }

   public boolean hasMappings() {
      return mappingEntries.length != 0;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      LCRDConsistentHash that = (LCRDConsistentHash) o;

      return consistentHash.equals(that.consistentHash) &&
            Arrays.equals(mappingEntries, that.mappingEntries);

   }

   @Override
   public int hashCode() {
      int result = consistentHash.hashCode();
      result = 31 * result + Arrays.hashCode(mappingEntries);
      return result;
   }

   @Override
   public String toString() {
      return "LCRDConsistentHash{" +
            "consistentHash=" + consistentHash +
            ", mappingEntries=" + Arrays.toString(mappingEntries) +
            '}';
   }

   private Address[] lookupKey(Object key) {
      if (mappingEntries.length == 0) {
         return null;
      } else if (mappingEntries.length == 1) {
         return mappingEntries[0].lookupKey(key);
      }
      List<Address> addressList = new ArrayList<Address>(Arrays.asList(mappingEntries[0].lookupKey(key)));
      for (int i = 1; i < mappingEntries.length; ++i) {
         for (Address address : mappingEntries[i].lookupKey(key)) {
            if (!addressList.contains(address)) {
               addressList.add(address);
            }
         }
      }
      return addressList.toArray(new Address[addressList.size()]);
   }

   private boolean isEmpty(String[] sortedTransactionClasses, LCRDCluster[] clusters) {
      int txClassLength = sortedTransactionClasses == null ? 0 : sortedTransactionClasses.length;
      int clusterLength = clusters == null ? 0 : clusters.length;
      if (txClassLength != clusterLength) {
         throw new IllegalArgumentException("Size mismatch between transaction classes and clusters");
      }
      return txClassLength == 0;
   }

   private boolean isKeyOwnByAddress(Address address, Object key) {
      Address[] addresses = lookupKey(key);
      return addresses != null && Arrays.asList(addresses).contains(address);
   }

   private static void writeMapEntry(ObjectOutput output, MappingEntry entry) throws IOException {
      output.writeInt(entry.sortedTransactionClasses.length);
      output.writeInt(entry.clusters.length);
      for (String s : entry.sortedTransactionClasses) {
         output.writeUTF(s);
      }
      for (LCRDCluster cluster : entry.clusters) {
         cluster.writeTo(output);
      }
   }

   private static MappingEntry readMapEntry(ObjectInput input) throws IOException, ClassNotFoundException {
      final String[] txClass = new String[input.readInt()];
      final LCRDCluster[] clusters = new LCRDCluster[input.readInt()];
      for (int i = 0; i < txClass.length; ++i) {
         txClass[i] = input.readUTF();
      }
      for (int i = 0; i < clusters.length; ++i) {
         clusters[i] = LCRDCluster.readFrom(input);
      }
      return new MappingEntry(txClass, clusters);
   }

   public static class Externalizer extends AbstractExternalizer<LCRDConsistentHash> {

      @Override
      public Integer getId() {
         return Ids.LCRD_CH;
      }

      @Override
      public Set<Class<? extends LCRDConsistentHash>> getTypeClasses() {
         return Util.<Class<? extends LCRDConsistentHash>>asSet(LCRDConsistentHash.class);
      }

      @Override
      public void writeObject(ObjectOutput output, LCRDConsistentHash object) throws IOException {
         output.writeObject(object.consistentHash);
         output.writeInt(object.mappingEntries.length);
         for (MappingEntry mappingEntry : object.mappingEntries) {
            writeMapEntry(output, mappingEntry);
         }
      }

      @Override
      public LCRDConsistentHash readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         ConsistentHash consistentHash = (ConsistentHash) input.readObject();
         int size = input.readInt();
         final MappingEntry[] mappingEntries = size == 0 ? EMPTY_MAPPING_ENTRY_ARRAY : new MappingEntry[size];
         for (int i = 0; i < size; ++i) {
            mappingEntries[i] = readMapEntry(input);
         }
         return new LCRDConsistentHash(consistentHash, mappingEntries);
      }
   }

   private static class MappingEntry {
      private final String[] sortedTransactionClasses;
      private final LCRDCluster[] clusters;

      private MappingEntry(String[] sortedTransactionClasses, LCRDCluster[] clusters) {
         this.sortedTransactionClasses = sortedTransactionClasses;
         this.clusters = clusters;
      }

      public final LCRDCluster clusterOf(String transactionClass) {
         int idx = Arrays.binarySearch(sortedTransactionClasses, transactionClass);
         return idx < 0 || idx >= clusters.length ? null : clusters[idx];
      }

      public final Address[] lookupKey(Object key) {
         if (key instanceof String) {
            LCRDCluster cluster = clusterOf((String) key);
            return cluster == null ? null : cluster.getMembers();
         }
         return null;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         MappingEntry that = (MappingEntry) o;

         return Arrays.equals(clusters, that.clusters) &&
               Arrays.equals(sortedTransactionClasses, that.sortedTransactionClasses);

      }

      @Override
      public int hashCode() {
         int result = Arrays.hashCode(sortedTransactionClasses);
         result = 31 * result + Arrays.hashCode(clusters);
         return result;
      }

      public void addToMap(Map<String, ExternalLCRDMappingEntry> map, int index, int length) {
         for (int i = 0; i < sortedTransactionClasses.length; ++i) {
            ExternalLCRDMappingEntry externalLCRDMappingEntry = map.get(sortedTransactionClasses[i]);
            if (externalLCRDMappingEntry == null) {
               externalLCRDMappingEntry = new ExternalLCRDMappingEntry(sortedTransactionClasses[i], length);
               map.put(sortedTransactionClasses[i], externalLCRDMappingEntry);
            }
            externalLCRDMappingEntry.set(index, clusters[i]);
         }
      }

      @Override
      public String toString() {
         return "MappingEntry{" +
               "sortedTransactionClasses=" + Arrays.toString(sortedTransactionClasses) +
               ", clusters=" + Arrays.toString(clusters) +
               '}';
      }
   }
}
