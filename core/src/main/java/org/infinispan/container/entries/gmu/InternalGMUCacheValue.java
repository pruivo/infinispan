package org.infinispan.container.entries.gmu;

import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.container.versioning.EntryVersion;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public interface InternalGMUCacheValue extends InternalCacheValue {

   /**
    * @return  complete version (from commit log) of when the version was created
    */
   EntryVersion getCreationVersion();

   /**
    * @return  the maximum version (from commit log) in each this value is valid
    */
   EntryVersion getMaximumValidVersion();

   /**
    * @return  the maximum version (from commit log) to update the transaction version
    */
   EntryVersion getMaximumTransactionVersion();

   /**
    * @return  true if this cache entry version is the most recent
    */
   boolean isMostRecent();

   /**
    * @return  the internal cache entry encapsulated by this instance
    */
   InternalCacheValue getInternalCacheValue();

}
