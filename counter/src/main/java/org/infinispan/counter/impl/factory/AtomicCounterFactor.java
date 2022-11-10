package org.infinispan.counter.impl.factory;

import java.util.concurrent.Executor;
import java.util.function.Supplier;

import org.infinispan.AdvancedCache;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.StrongCounter;
import org.infinispan.counter.api.WeakCounter;
import org.infinispan.counter.impl.atomic.BoundedAtomicStrongCounter;
import org.infinispan.counter.impl.atomic.UnboundedAtomicStrongCounter;
import org.infinispan.counter.impl.entries.CounterKey;
import org.infinispan.counter.impl.entries.CounterValue;
import org.infinispan.counter.impl.weak.WeakCounterImpl;

/**
 * //TODO document this!
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class AtomicCounterFactor extends CacheBasedCounter implements CounterFactory {



   public AtomicCounterFactor(Supplier<AdvancedCache<CounterKey, CounterValue>> cacheSupplier,
         Executor executor) {
      super(cacheSupplier, executor);
   }


   @Override
   public StrongCounter newUnboundedCounter(String counterName, CounterConfiguration configuration) {
      return new UnboundedAtomicStrongCounter(counterName, configuration, getNotificationManager());
   }

   @Override
   public StrongCounter newBoundedCounter(String counterName, CounterConfiguration configuration) {
      return new BoundedAtomicStrongCounter(counterName, configuration, getNotificationManager());
   }

   @Override
   public WeakCounter newWeakCounter(String counterName, CounterConfiguration configuration) {
      initNotificationManager();
      WeakCounterImpl counter = new WeakCounterImpl(counterName, cache(configuration), configuration,
            getNotificationManager());
      counter.init();
      return counter;
   }

   @Override
   public void stop() {
      getNotificationManager().stop();
   }
}
