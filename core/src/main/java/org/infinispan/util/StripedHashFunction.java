package org.infinispan.util;

import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.configuration.cache.Configuration;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class StripedHashFunction<T> {

   private final Equivalence<T> equivalence;
   private final int lockSegmentMask;
   private final int lockSegmentShift;
   private final int numSegments;

   public StripedHashFunction(Equivalence<T> equivalence, int concurrencyLevel) {
      this.equivalence = equivalence;
      int tempLockSegShift = 0;
      int tmpNumSegments = 1;
      while (tmpNumSegments < concurrencyLevel) {
         ++tempLockSegShift;
         tmpNumSegments <<= 1;
      }
      lockSegmentShift = 32 - tempLockSegShift;
      lockSegmentMask = tmpNumSegments - 1;
      numSegments = tmpNumSegments;
   }

   @SuppressWarnings("unchecked")
   public static <T> StripedHashFunction<T> buildFromConfiguration(Configuration configuration) {
      return new StripedHashFunction<>((Equivalence<T>) configuration.dataContainer().keyEquivalence(),
                                        configuration.locking().concurrencyLevel());
   }

   /**
    * Returns a hash code for non-null Object x. Uses the same hash code spreader as most other java.util hash tables,
    * except that this uses the string representation of the object passed in.
    *
    * @param hashCode the object's hash code serving as a key.
    * @return the hash code
    */
   private static int hash(int hashCode) {
      int h = hashCode;
      h += ~(h << 9);
      h ^= (h >>> 14);
      h += (h << 4);
      h ^= (h >>> 10);
      return h;
   }

   public final int getNumSegments() {
      return numSegments;
   }

   public final int hashToSegment(T object) {
      return (hash(equivalence.hashCode(object)) >>> lockSegmentShift) & lockSegmentMask;
   }
}
