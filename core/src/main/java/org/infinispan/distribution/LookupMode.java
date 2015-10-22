package org.infinispan.distribution;

import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.topology.CacheTopology;

import static java.util.Objects.requireNonNull;

/**
 * It tells which consistent hash should be used to determine the ownership of a given key.
 *
 * @author Pedro Ruivo
 * @since 8.0
 * @see {@link DistributionManager#locate(Object, LookupMode)}
 */
public enum LookupMode {

   /**
    * It uses the read consistent hash to find the key owners
    */
   READ {
      @Override
      public ConsistentHash getConsistentHash(CacheTopology cacheTopology) {
         return requireNonNull(cacheTopology, "Cache Topology can't be null.").getReadConsistentHash();
      }
   },

   /**
    * It uses the write consistent hash to find the key owners
    */
   WRITE {
      @Override
      public ConsistentHash getConsistentHash(CacheTopology cacheTopology) {
         return requireNonNull(cacheTopology, "Cache Topology can't be null.").getWriteConsistentHash();
      }
   };

   /**
    * @return the correct consistent hash from the cache topology
    */
   public abstract ConsistentHash getConsistentHash(CacheTopology cacheTopology);

}
