package org.infinispan.stats.container;

import java.util.Arrays;

/**
 * //TODO: document this!
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
public class EnumStatisticsContainer<K extends Enum<K>> {

   private final Class<K> keyType;
   private final long[] values;

   public EnumStatisticsContainer(Class<K> keyType) {
      this.keyType = keyType;
      K[] enumConstants = keyType.getEnumConstants();
      values = new long[enumConstants.length];
      for (int i = 0; i < values.length; ++i) {
         values[i] = 0;
      }
   }

   public final int size() {
      return values.length;
   }

   public final long get(K key) {
      return values[key.ordinal()];
   }

   public final long put(K key, long value) {
      long oldVal = values[key.ordinal()];
      values[key.ordinal()] = value;
      return oldVal;
   }

   public final long remove(K key) {
      return put(key, 0L);
   }

   public final void add(K key, long value) {
      values[key.ordinal()] += value;
   }

   public final void increment(K key) {
      add(key, 1L);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      EnumStatisticsContainer that = (EnumStatisticsContainer) o;

      return keyType.equals(that.keyType) && Arrays.equals(values, that.values);

   }

   @Override
   public int hashCode() {
      int result = keyType.hashCode();
      result = 31 * result + Arrays.hashCode(values);
      return result;
   }

   @Override
   public String toString() {
      StringBuilder builder = new StringBuilder("EnumStatisticsContainer{");
      K[] enumConstants = keyType.getEnumConstants();
      for (int i = 0; i < enumConstants.length; ++i) {
         builder.append(enumConstants[i]).append("=").append(values[i]).append(",");
      }
      if (builder.charAt(builder.length() - 1) == ',') {
         builder.setCharAt(builder.length() - 1, '}');
      } else {
         builder.append("}");
      }
      return builder.toString();
   }
}
