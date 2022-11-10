package org.infinispan.counter.impl.atomic;

import org.infinispan.counter.api.CounterState;

/**
 * //TODO document this!
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class BoundedCounterHolder implements CounterHolder {

   private volatile long value;
   private volatile CounterState state;

   BoundedCounterHolder(long initialValue) {
      this.value = initialValue;
      this.state = CounterState.VALID;
   }

   @Override
   public long getValue() {
      return value;
   }

   @Override
   public CounterState getState() {
      return state;
   }

   @Override
   public CounterHolder setValue(long value) {
      this.value = value;
      return this;
   }

   @Override
   public CounterHolder setState(CounterState state) {
      this.state = state;
      return this;
   }
}
