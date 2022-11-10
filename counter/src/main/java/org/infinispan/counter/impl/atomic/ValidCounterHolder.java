package org.infinispan.counter.impl.atomic;

import org.infinispan.counter.api.CounterState;

/**
 * //TODO document this!
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class ValidCounterHolder implements CounterHolder {

   private volatile long value;

   ValidCounterHolder(long initialValue) {
      this.value = initialValue;
   }

   @Override
   public long getValue() {
      return value;
   }

   @Override
   public CounterState getState() {
      return CounterState.VALID;
   }

   @Override
   public CounterHolder setValue(long value) {
      this.value = value;
      return this;
   }

   @Override
   public CounterHolder setState(CounterState state) {
      return this;
   }
}
