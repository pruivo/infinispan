package org.infinispan.container.entries;

import org.infinispan.container.DataContainer;
import org.infinispan.metadata.Metadata;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class ClearCacheEntry<K, V> implements CacheEntry<K, V> {

   private static final ClearCacheEntry INSTANCE = new ClearCacheEntry();

   public static <K,V> ClearCacheEntry<K, V> getInstance() {
      //noinspection unchecked
      return INSTANCE;
   }

   private ClearCacheEntry() {}

   @Override
   public boolean isNull() {
      return true;
   }

   @Override
   public boolean isChanged() {
      return true;
   }

   @Override
   public void setChanged(boolean changed) {
      /*no-op*/
   }

   @Override
   public boolean isCreated() {
      return false;
   }

   @Override
   public void setCreated(boolean created) {
      /*no-op*/
   }

   @Override
   public boolean isRemoved() {
      return true;
   }

   @Override
   public void setRemoved(boolean removed) {
      /*no-op*/
   }

   @Override
   public boolean isEvicted() {
      return false;
   }

   @Override
   public void setEvicted(boolean evicted) {
      /*no-op*/
   }

   @Override
   public boolean isValid() {
      return true;
   }

   @Override
   public void setValid(boolean valid) {
      /*no-op*/
   }

   @Override
   public boolean isLoaded() {
      return false;
   }

   @Override
   public void setLoaded(boolean loaded) {
      /*no-op*/
   }

   @Override
   public K getKey() {
      return null;
   }

   @Override
   public V getValue() {
      return null;
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
   public boolean skipLookup() {
      return true;
   }

   @Override
   public V setValue(V value) {
      /*-no-op*/
      return null;
   }

   @Override
   public void commit(DataContainer<K, V> container, Metadata metadata) {
      container.clear();
   }

   @Override
   public void rollback() {
      /*no-op*/
   }

   @Override
   public void setSkipLookup(boolean skipLookup) {
      /*no-op*/
   }

   @Override
   public boolean undelete(boolean doUndelete) {
      return false;
   }

   @Override
   public CacheEntry<K, V> clone() {
      try {
         //noinspection unchecked
         return (CacheEntry<K, V>) super.clone();
      } catch (CloneNotSupportedException e) {
         throw new IllegalStateException(e);
      }
   }

   @Override
   public Metadata getMetadata() {
      return null;
   }

   @Override
   public void setMetadata(Metadata metadata) {
      /*no-op*/
   }

   @Override
   public String toString() {
      return "ClearCacheEntry{}";
   }
}
