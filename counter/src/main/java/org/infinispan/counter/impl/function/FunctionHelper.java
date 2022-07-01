package org.infinispan.counter.impl.function;

import static org.infinispan.counter.impl.entries.CounterValue.newCounterValue;

import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterState;
import org.infinispan.counter.api.CounterType;
import org.infinispan.counter.impl.Utils;
import org.infinispan.counter.impl.entries.CounterValue;
import org.infinispan.functional.MetaParam;
import org.infinispan.functional.impl.CounterConfigurationMetaParam;
import org.infinispan.functional.EntryView;

/**
 * A helper with the function logic.
 * <p>
 * It avoids duplicate code between the functions and the create-functions.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
final class FunctionHelper {

   private FunctionHelper() {
   }

   /**
    * Performs a compare-and-swap and return the previous value.
    * <p>
    * The compare-and-swap is successful if the return value is equals to the {@code expected}.
    *
    * @return the previous value or {@link CounterState#UPPER_BOUND_REACHED}/{@link CounterState#LOWER_BOUND_REACHED} if
    * it reaches the boundaries of a bounded counter.
    */
   static Object compareAndSwap(EntryView.ReadWriteEntryView<?, CounterValue> entry,
         CounterValue value, CounterConfigurationMetaParam metadata, long expected, long update) {
      switch (Utils.compareAndSwap(metadata.get(), value, expected, update)) {
         case OK:
            setInEntry(entry, newCounterValue(update), metadata);
            return value.getValue();
         case FAILED_WRONG_EXPECTED:
            return value.getValue();
         case FAILED_LOWER_BOUND_REACHED:
            return CounterState.LOWER_BOUND_REACHED;
         case FAILED_UPPER_BOUND_REACHED:
            return CounterState.UPPER_BOUND_REACHED;
         default:
            throw new IllegalStateException();
      }
   }

   static CounterValue add(EntryView.ReadWriteEntryView<?, CounterValue> entry,
         CounterValue value, CounterConfigurationMetaParam metadata, long delta) {
      CounterValue newValue = Utils.add(metadata.get(), value, delta);
      if (newValue.equals(value)) {
         return value;
      }
      setInEntry(entry, newValue, metadata);
      return newValue;
   }

   private static void setInEntry(EntryView.ReadWriteEntryView<?, CounterValue> entry, CounterValue value, CounterConfigurationMetaParam metadata) {
      CounterConfiguration configuration = metadata.get();
      if (configuration.type() != CounterType.WEAK && configuration.lifespan() > 0) {
         entry.set(value, metadata, new MetaParam.MetaLifespan(configuration.lifespan()), new MetaParam.MetaUpdateCreationTime(false));
      } else {
         entry.set(value, metadata);
      }
   }
}
