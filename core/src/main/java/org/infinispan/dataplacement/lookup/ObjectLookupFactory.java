package org.infinispan.dataplacement.lookup;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.dataplacement.SegmentMapping;

import java.util.Collection;

/**
 * Interface that creates the Object Lookup instances based on the keys to be moved
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public interface ObjectLookupFactory {

   /**
    * sets the Infinispan configuration to configure this object lookup factory
    *
    * @param configuration the Infinispan configuration
    */
   void setConfiguration(Configuration configuration);

   /**
    * creates the {@link ObjectLookup} corresponding to the keys to be moved
    *
    * @param keysToMove     the keys to move and the new owners
    * @param numberOfOwners the number of owners (a.k.a. replication degree)
    * @return the object lookup or null if it is not possible to create it
    */
   ObjectLookup createObjectLookup(SegmentMapping keysToMove, int numberOfOwners);

   /**
    * init the object lookup
    *
    * @param objectLookup the object lookup
    */
   void init(Collection<ObjectLookup> objectLookup);

   /**
    * returns the number of phases when the query profiling
    *
    * @return the number of phases when the query profiling
    */
   int getNumberOfQueryProfilingPhases();
}
