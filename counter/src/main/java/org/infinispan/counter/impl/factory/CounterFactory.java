package org.infinispan.counter.impl.factory;

import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.StrongCounter;
import org.infinispan.counter.api.WeakCounter;

/**
 * //TODO document this!
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public interface CounterFactory {

   StrongCounter newUnboundedCounter(String counterName, CounterConfiguration configuration);
   StrongCounter newBoundedCounter(String counterName, CounterConfiguration configuration);
   WeakCounter newWeakCounter(String counterName, CounterConfiguration configuration);
   void stop();
}
