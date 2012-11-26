package org.infinispan.container.entries.gmu;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.versioning.EntryVersion;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public interface InternalGMUCacheEntry extends InternalCacheEntry {

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

   void setCreationVersion(EntryVersion entryVersion);

   void setMaximumValidVersion(EntryVersion version);

   /**
    * @return  true if this cache entry version is the most recent
    */
   boolean isMostRecent();

   /**
    * @return  the internal cache entry encapsulated by this instance
    */
   InternalCacheEntry getInternalCacheEntry();

}
