package org.infinispan.container.entries.gmu;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.container.versioning.EntryVersion;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class InternalGMURemovedCacheValue implements InternalGMUCacheValue {

   protected final EntryVersion version;

   public InternalGMURemovedCacheValue(EntryVersion version) {
      this.version = version;
   }

   @Override
   public EntryVersion getCreationVersion() {
      return null;
   }

   @Override
   public EntryVersion getMaximumValidVersion() {
      return null;
   }

   @Override
   public EntryVersion getMaximumTransactionVersion() {
      return null;
   }

   @Override
   public boolean isMostRecent() {
      return false;
   }

   @Override
   public InternalCacheValue getInternalCacheValue() {
      return null;
   }

   @Override
   public Object getValue() {
      return null;
   }

   @Override
   public InternalCacheEntry toInternalCacheEntry(Object key) {
      return new InternalGMURemovedCacheEntry(key, version);
   }

   @Override
   public boolean isExpired(long now) {
      return false;
   }

   @Override
   public boolean isExpired() {
      return false;
   }

   @Override
   public boolean canExpire() {
      return false;
   }

   @Override
   public long getCreated() {
      return -1;
   }

   @Override
   public long getLastUsed() {
      return -1;
   }

   @Override
   public long getLifespan() {
      return -1;
   }

   @Override
   public long getMaxIdle() {
      return -1;
   }

   @Override
   public String toString() {
      return "InternalGMURemovedCacheValue{" +
            "version=" + version +
            '}';
   }
}
