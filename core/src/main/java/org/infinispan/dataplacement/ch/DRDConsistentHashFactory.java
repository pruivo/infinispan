package org.infinispan.dataplacement.ch;

import org.infinispan.commons.hash.Hash;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.ConsistentHashFactory;
import org.infinispan.distribution.ch.DefaultConsistentHashFactory;
import org.infinispan.remoting.transport.Address;

import java.util.List;

/**
 * @author Pedro Ruivo
 * @since 5.2
 */
public class DRDConsistentHashFactory implements ConsistentHashFactory<DRDConsistentHash> {

   private final ConsistentHashFactory consistentHashFactory;

   public DRDConsistentHashFactory() {
      consistentHashFactory = new DefaultConsistentHashFactory();
   }

   @Override
   public DRDConsistentHash create(Hash hashFunction, int numOwners, int numSegments, List<Address> members) {
      return new DRDConsistentHash(consistentHashFactory.create(hashFunction, numOwners, numSegments, members));
   }

   @Override
   public DRDConsistentHash updateMembers(DRDConsistentHash baseCH, List<Address> newMembers) {
      ConsistentHash updatedConsistentHash = consistentHashFactory.updateMembers(baseCH.getConsistentHash(), newMembers);
      DRDConsistentHash updatedDrdConsistentHash;
      if (!baseCH.hasMappings()) {
         updatedDrdConsistentHash = new DRDConsistentHash(updatedConsistentHash);
      } else {
         updatedDrdConsistentHash = null;
      }
      return baseCH.equals(updatedConsistentHash) ? baseCH : updatedDrdConsistentHash;
   }

   @Override
   public DRDConsistentHash rebalance(DRDConsistentHash baseCH, Object customData) {
      return null;  //To change body of implemented methods use File | Settings | File Templates.
   }

   @Override
   public DRDConsistentHash union(DRDConsistentHash ch1, DRDConsistentHash ch2) {
      return null;  //To change body of implemented methods use File | Settings | File Templates.
   }
}
