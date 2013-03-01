package org.infinispan.dataplacement.stats;

/**
 * Implements an incrementable long
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class IncrementableLong {

   private long value = 0;

   public final void increment() {
      value++;
   }

   public final void add(Number value) {
      this.value += value.longValue();
   }

   public final void add(IncrementableLong value) {
      this.value += value.value;
   }

   public long getValue() {
      return value;
   }

   @Override
   public String toString() {
      return Long.toString(value);
   }
}
