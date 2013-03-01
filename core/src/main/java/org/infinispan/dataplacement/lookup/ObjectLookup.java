package org.infinispan.dataplacement.lookup;

import org.infinispan.dataplacement.stats.IncrementableLong;

import java.io.Serializable;
import java.util.List;

/**
 * An interface that is used to query for the new owner index defined by the data placement optimization
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public interface ObjectLookup extends Serializable {

   /**
    * queries this object lookup for the node index where the key can be (if the keys is moved)
    *
    * @param key the key to find
    * @return the owners index where the key is or null if the key was not moved
    */
   List<Integer> query(Object key);

   /**
    * the same as {@link #query(Object)} but it profiling information
    *
    * @param key            the key to find
    * @param phaseDurations the array with the duration of the phase (in nanoseconds)
    * @return the owners index where the key is or null if the key was not moved
    */
   List<Integer> queryWithProfiling(Object key, IncrementableLong[] phaseDurations);
}
