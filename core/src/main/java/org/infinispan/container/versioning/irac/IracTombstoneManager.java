package org.infinispan.container.versioning.irac;

/**
 * Stores and manages tombstones for removed keys.
 * <p>
 * It manages the tombstones for IRAC protocol. Tombstones are used when a key is removed but the version/metadata is
 * required to perform conflict or duplicates detection.
 * <p>
 * Tombstones are removed when they are not required by any site or its value is updated (with a non-null value).
 *
 * @since 14.0
 */
public interface IracTombstoneManager {


   /**
    * @return the number of tombstones stored.
    */
   int size();

   /**
    * @return {@code true} if the cleanup task is currently running.
    */
   boolean isTaskRunning();

   /**
    * @return The current delay between cleanup task in milliseconds.
    */
   long getCurrentDelayMillis();

}
