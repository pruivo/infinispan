package org.infinispan.util;

import java.util.Arrays;

import org.infinispan.commons.marshall.AdvancedExternalizer;

/**
 * A hash map implementation of {@link IntObjectMap} that uses open addressing for keys.
 * To minimize the memory footprint, this class uses open addressing rather than chaining.
 * Collisions are resolved using linear probing. Deletions implement compaction, so cost of
 * remove can approach O(N) for full maps, which makes a small loadFactor recommended.
 * <p>
 * Extracted from The Netty Project.
 * * @since 9.0
 */
public final class ClassToExternalizerHashMap {

   /**
    * Default initial capacity. Used if not specified in the constructor
    */
   private static final int DEFAULT_CAPACITY = 8;

   /**
    * Default load factor. Used if not specified in the constructor
    */
   private static final float DEFAULT_LOAD_FACTOR = 0.5f;


   /**
    * The maximum number of elements allowed without allocating more space.
    */
   private int maxSize;

   /**
    * The load factor for the map. Used to calculate {@link #maxSize}.
    */
   private final float loadFactor;

   private Class[] keys;
   private AdvancedExternalizer[] values;
   private int size;
   private int mask;

   public ClassToExternalizerHashMap() {
      this(DEFAULT_CAPACITY, DEFAULT_LOAD_FACTOR);
   }

   public ClassToExternalizerHashMap(int initialCapacity) {
      this(initialCapacity, DEFAULT_LOAD_FACTOR);
   }

   public ClassToExternalizerHashMap(int initialCapacity, float loadFactor) {
      if (initialCapacity < 1) {
         throw new IllegalArgumentException("initialCapacity must be >= 1");
      }
      if (loadFactor <= 0.0f || loadFactor > 1.0f) {
         // Cannot exceed 1 because we can never store more than capacity elements;
         // using a bigger loadFactor would trigger rehashing before the desired load is reached.
         throw new IllegalArgumentException("loadFactor must be > 0 and <= 1");
      }

      this.loadFactor = loadFactor;

      // Adjust the initial capacity if necessary.
      int capacity = findNextPositivePowerOfTwo(initialCapacity);
      mask = capacity - 1;

      // Allocate the arrays.
      keys = new Class[capacity];
      @SuppressWarnings({"unchecked", "SuspiciousArrayCast"})
      AdvancedExternalizer[] temp = new AdvancedExternalizer[capacity];
      values = temp;

      // Initialize the maximum size value.
      maxSize = calcMaxSize(capacity);
   }

   public AdvancedExternalizer get(Class key) {
      int index = indexOf(key);
      return index == -1 ? null : values[index];
   }

   public AdvancedExternalizer put(Class key, AdvancedExternalizer value) {
      int startIndex = hashIndex(key);
      int index = startIndex;

      for (; ; ) {
         if (keys[index] == null) {
            // Found empty slot, use it.
            keys[index] = key;
            values[index] = value;
            growSize();
            return null;
         }
         if (keys[index] == key) {
            // Found existing entry with this key, just replace the value.
            AdvancedExternalizer previousValue = values[index];
            values[index] = value;
            return previousValue;
         }

         // Conflict, keep probing ...
         if ((index = probeNext(index)) == startIndex) {
            // Can only happen if the map was full at MAX_ARRAY_SIZE and couldn't grow.
            throw new IllegalStateException("Unable to insert");
         }
      }
   }

   private int probeNext(int index) {
      return index == values.length - 1 ? 0 : index + 1;
   }

   public void clear() {
      Arrays.fill(keys, null);
      Arrays.fill(values, null);
      size = 0;
   }

   /**
    * Locates the index for the given key. This method probes using double hashing.
    *
    * @param key the key for an entry in the map.
    * @return the index where the key was found, or {@code -1} if no entry is found for that key.
    */
   private int indexOf(Class key) {
      int startIndex = hashIndex(key);
      int index = startIndex;

      for (; ; ) {
         if (keys[index] == null) {
            // It's available, so no chance that this value exists anywhere in the map.
            return -1;
         }
         if (key == keys[index]) {
            return index;
         }

         // Conflict, keep probing ...
         if ((index = probeNext(index)) == startIndex) {
            return -1;
         }
      }
   }

   /**
    * Returns the hashed index for the given key.
    */
   private int hashIndex(Class key) {
      return key.hashCode() & mask;
   }

   /**
    * Grows the map size after an insertion. If necessary, performs a rehash of the map.
    */
   private void growSize() {
      size++;

      if (size > maxSize) {
         if (keys.length == Integer.MAX_VALUE) {
            throw new IllegalStateException("Max capacity reached at size=" + size);
         }

         // Double the capacity.
         rehash(keys.length << 1);
      }
   }

   /**
    * Calculates the maximum size allowed before rehashing.
    */
   private int calcMaxSize(int capacity) {
      // Clip the upper bound so that there will always be at least one available slot.
      int upperBound = capacity - 1;
      return Math.min(upperBound, (int) (capacity * loadFactor));
   }

   /**
    * Rehashes the map for the given capacity.
    *
    * @param newCapacity the new capacity for the map.
    */
   private void rehash(int newCapacity) {
      Class[] oldKeys = keys;
      AdvancedExternalizer[] oldVals = values;

      keys = new Class[newCapacity];
      @SuppressWarnings({"unchecked", "SuspiciousArrayCast"})
      AdvancedExternalizer[] temp = new AdvancedExternalizer[newCapacity];
      values = temp;

      maxSize = calcMaxSize(newCapacity);
      mask = newCapacity - 1;

      // Insert to the new arrays.
      for (int i = 0; i < oldVals.length; ++i) {
         AdvancedExternalizer oldVal = oldVals[i];
         if (oldVal != null) {
            // Inlined put(), but much simpler: we don't need to worry about
            // duplicated keys, growing/rehashing, or failing to insert.
            Class oldKey = oldKeys[i];
            int index = hashIndex(oldKey);

            for (; ; ) {
               if (values[index] == null) {
                  keys[index] = oldKey;
                  values[index] = oldVal;
                  break;
               }

               // Conflict, keep probing. Can wrap around, but never reaches startIndex again.
               index = probeNext(index);
            }
         }
      }
   }

   @Override
   public String toString() {
      if (size == 0) {
         return "{}";
      }
      StringBuilder sb = new StringBuilder(4 * size);
      sb.append('{');
      boolean first = true;
      for (int i = 0; i < values.length; ++i) {
         AdvancedExternalizer value = values[i];
         if (value != null) {
            if (!first) {
               sb.append(", ");
            }
            sb.append(keys[i]).append('=').append(value);
            first = false;
         }
      }
      return sb.append('}').toString();
   }

   /**
    * Fast method of finding the next power of 2 greater than or equal to the supplied value.
    * <p>
    * If the value is {@code <= 0} then 1 will be returned.
    * This method is not suitable for {@link Integer#MIN_VALUE} or numbers greater than 2^30.
    *
    * @param value from which to search for next power of 2
    * @return The next power of 2 or the value itself if it is a power of 2
    */
   private static int findNextPositivePowerOfTwo(final int value) {
      assert value > Integer.MIN_VALUE && value < 0x40000000;
      return 1 << (32 - Integer.numberOfLeadingZeros(value - 1));
   }

}
