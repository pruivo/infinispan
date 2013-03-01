package org.infinispan.dataplacement;

import org.infinispan.commons.hash.Hash;
import org.infinispan.distribution.TestAddress;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Cluster Snapshot testing
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", testName = "dataplacement.ClusterSnapshotTest")
public class ClusterSnapshotTest {

   private static final Log log = LogFactory.getLog(ClusterSnapshotTest.class);
   private final TestHash HASH_FUNCTION = new TestHash();

   public void testEmpty() {
      ClusterSnapshot clusterSnapshot = createClusterSnapshot(0, new LinkedList<Address>());

      assert !clusterSnapshot.contains(new TestAddress(0));
      assert clusterSnapshot.get(0) == null;
      assert clusterSnapshot.get(-10) == null;
      assert clusterSnapshot.indexOf(new TestAddress(1)) == -1;
   }

   public void testGet() {
      ArrayList<Address> members = new ArrayList<Address>();
      ClusterSnapshot clusterSnapshot = createClusterSnapshot(100, members);

      for (Address address : members) {
         assert clusterSnapshot.contains(address);
         int index = clusterSnapshot.indexOf(address);
         assert index != -1;
         assert address.equals(clusterSnapshot.get(index));
      }
   }

   public void testContainsAll() {
      ArrayList<Address> members = new ArrayList<Address>(100);
      ClusterSnapshot snapshot = createClusterSnapshot(100, members);

      for (Address address : members) {
         assert snapshot.contains(address);
         assert snapshot.indexOf(address) != -1;
      }
   }

   public void testNotContains() {
      ArrayList<Address> members = new ArrayList<Address>(100);
      ClusterSnapshot snapshot = createClusterSnapshot(100, members);
      assert !snapshot.contains(new TestAddress(101));
      assert snapshot.indexOf(new TestAddress(101)) == -1;
      assert snapshot.get(101) == null;
   }

   public void testSameIndex() {
      ArrayList<Address> members = new ArrayList<Address>(100);
      ClusterSnapshot snapshot = createClusterSnapshot(100, members);
      Address address = new TestAddress(10);

      assert snapshot.contains(address);
      int index = snapshot.indexOf(address);

      for (int i = 0; i < 10; i++) {
         assert index == snapshot.indexOf(address);
      }
   }

   public void testNonContainsConflicting() {
      ArrayList<Address> members = new ArrayList<Address>(100);
      ClusterSnapshot snapshot = createClusterSnapshot(100, members);
      Address address = new TestAddress(101);
      Address conflicting = new TestAddress(-101);

      assert HASH_FUNCTION.hash(address) == HASH_FUNCTION.hash(conflicting);
      assert !address.equals(conflicting);

      assert !snapshot.contains(conflicting);
      assert snapshot.indexOf(conflicting) == -1;
   }

   public void testContainsConflicting() {
      ArrayList<Address> members = new ArrayList<Address>(100);

      createConflictingHash(50, members);
      ClusterSnapshot snapshot = createClusterSnapshot(50, members);

      Address address = new TestAddress(10);
      Address conflicting = new TestAddress(-10);

      assert HASH_FUNCTION.hash(address) == HASH_FUNCTION.hash(conflicting);
      assert !address.equals(conflicting);

      assert snapshot.contains(conflicting);
      assert snapshot.contains(address);

      int aIndex = snapshot.indexOf(address);
      int cIndex = snapshot.indexOf(conflicting);

      assert aIndex != -1;
      assert cIndex != -1;
      assert aIndex != cIndex;
   }

   public void testSize() {
      ClusterSnapshot snapshot = createClusterSnapshot(10, new LinkedList<Address>());
      assert snapshot.size() == 10;

      snapshot = createClusterSnapshot(15, new LinkedList<Address>());
      assert snapshot.size() == 15;

      snapshot = createClusterSnapshot(0, new LinkedList<Address>());
      assert snapshot.size() == 0;
   }

   public void testPerformance() {
      int size = 10000;
      ArrayList<Address> members = new ArrayList<Address>();
      ClusterSnapshot snapshot = createClusterSnapshot(size, members);

      performance(members, snapshot, "0% Hash Conflicts");

      members.clear();
      for (int i = 0; i < size / 2; ++i) {
         members.add(new SameHashAddress(i));
      }
      snapshot = createClusterSnapshot(size / 2, members);

      performance(members, snapshot, "50% Hash Conflicts");

      members.clear();
      for (int i = 0; i < size; ++i) {
         members.add(new SameHashAddress(i));
      }
      snapshot = createClusterSnapshot(0, members);

      performance(members, snapshot, "100% Hash Conflicts");
   }

   private void performance(ArrayList<Address> members, ClusterSnapshot snapshot, String title) {
      for (int iteration = 0; iteration < 5; ++iteration) {
         long start = System.currentTimeMillis();
         for (Address address : members) {
            int index = members.indexOf(address);
            assert index != -1;
         }
         long end = System.currentTimeMillis();
         long arrayListDuration = end - start;

         start = System.currentTimeMillis();
         for (Address address : members) {
            int index = snapshot.indexOf(address);
            assert index != -1;
         }
         end = System.currentTimeMillis();
         long clusterSnapshotDuration = end - start;

         String msg = String.format("Performance results[%s,%s]: Array List: %s ms, Cluster Snapshot: %s ms", iteration,
                                    title, arrayListDuration, clusterSnapshotDuration);
         log.info(msg);
      }
   }

   private void createConflictingHash(int size, List<Address> members) {
      for (int i = 0; i < size; i++) {
         members.add(new TestAddress(i * -1));
      }
   }

   private ClusterSnapshot createClusterSnapshot(int size, List<Address> members) {
      for (int i = 0; i < size; ++i) {
         members.add(new TestAddress(i));
      }
      Address[] membersArray = members.toArray(new Address[size]);
      ClusterSnapshot clusterSnapshot = new ClusterSnapshot(membersArray, HASH_FUNCTION);
      log.tracef("Created cluster snapshot: %s", clusterSnapshot);
      return clusterSnapshot;
   }

   private class TestHash implements Hash {

      @Override
      public int hash(byte[] payload) {
         return 0;
      }

      @Override
      public int hash(int hashcode) {
         return hashcode;
      }

      @Override
      public int hash(Object o) {
         if (o instanceof SameHashAddress) {
            return 0;
         }
         return Math.abs(o.hashCode());
      }
   }

   private class SameHashAddress extends TestAddress {
      private SameHashAddress(int addressNum) {
         super(addressNum);
      }
   }

}
