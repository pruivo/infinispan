package org.infinispan.counter.impl.atomic;

import org.infinispan.counter.api.CounterState;

/**
 * //TODO document this!
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public interface CounterHolder {

   long getValue();

   CounterState getState();

   CounterHolder setValue(long value);

   CounterHolder setState(CounterState state);



}
