package org.infinispan.container.versioning.gmu;

import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.InequalVersionComparisonResult;
import org.infinispan.remoting.transport.Address;

import java.util.Set;
import java.util.TreeSet;

import static org.infinispan.container.versioning.InequalVersionComparisonResult.BEFORE;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class GMUReadVersion extends GMUVersion {

   private final long version;
   private final Set<Pair> notVisibleSubVersions;

   public GMUReadVersion(String cacheName, int viewId, GMUVersionGenerator versionGenerator, long version) {
      super(cacheName, viewId, versionGenerator);
      this.version = version;
      this.notVisibleSubVersions = new TreeSet<Pair>();
   }

   @Override
   public final long getVersionValue(Address address) {
      return getThisNodeVersionValue();
   }

   @Override
   public final long getVersionValue(int addressIndex) {
      return getThisNodeVersionValue();
   }

   @Override
   public long getThisNodeVersionValue() {
      return version;
   }

   public final void addNotVisibleSubversion(long version, int subVersion) {
      notVisibleSubVersions.add(new Pair(version, subVersion));
   }

   public final boolean contains(long version, int subVersion) {
      return notVisibleSubVersions.contains(new Pair(version, subVersion));
   }

   @Override
   public InequalVersionComparisonResult compareTo(EntryVersion other) {
      //only comparable with GMU cache entry version
      if (other == null) {
         return BEFORE;
      } else if (other instanceof GMUCacheEntryVersion) {
         GMUCacheEntryVersion cacheEntryVersion = (GMUCacheEntryVersion) other;
         if (contains(cacheEntryVersion.getThisNodeVersionValue(), cacheEntryVersion.getSubVersion())) {
            //the cache entry is an invalid version.
            return BEFORE;
         }
         return compare(version, cacheEntryVersion.getThisNodeVersionValue());
      }
      throw new IllegalArgumentException("Cannot compare " + getClass() + " with " + other.getClass());
   }

   @Override
   public String toString() {
      return "GMUReadVersion{" +
            "version=" + version +
            ", notVisibleSubVersions=" + notVisibleSubVersions +
            ", " + super.toString();
   }

   private class Pair implements Comparable<Pair> {
      private final long version;
      private final int subVersion;

      private Pair(long version, int subVersion) {
         this.version = version;
         this.subVersion = subVersion;
      }

      @Override
      public int compareTo(Pair pair) {
         int result = Long.valueOf(version).compareTo(pair.version);
         if (result == 0) {
            return Integer.valueOf(subVersion).compareTo(pair.subVersion);
         }
         return result;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         Pair pair = (Pair) o;
         return subVersion == pair.subVersion && version == pair.version;

      }

      @Override
      public int hashCode() {
         int result = (int) (version ^ (version >>> 32));
         result = 31 * result + subVersion;
         return result;
      }

      @Override
      public String toString() {
         return "(" + version + "," + subVersion + ")";
      }
   }
}
