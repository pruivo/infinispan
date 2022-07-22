package org.infinispan.counter.impl.jgroups;

import static org.infinispan.counter.exception.CounterOutOfBoundsException.LOWER_BOUND;
import static org.infinispan.counter.exception.CounterOutOfBoundsException.UPPER_BOUND;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.logging.Log;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.impl.Utils;
import org.infinispan.counter.impl.entries.CounterValue;
import org.infinispan.util.logging.LogFactory;
import org.jgroups.blocks.atomic.AsyncCounter;

/**
 * TODO! document this
 */
public class JGroupsBoundedStrongCounter extends JGroupsUnboundedStrongCounter {

   private static final Log log = LogFactory.getLog(JGroupsBoundedStrongCounter.class, Log.class);

   public JGroupsBoundedStrongCounter(String name, CounterConfiguration configuration, AsyncCounter counter) {
      super(name, configuration, counter);
   }

   @Override
   public CompletableFuture<Long> addAndGet(long delta) {
      if (delta == 0) {
         return getValue();
      }
      return getValue().thenCompose(value -> addAndCheckBound(CounterValue.newCounterValue(value), delta));
   }

   @Override
   public CompletableFuture<Long> compareAndSwap(long expect, long update) {
      return super.compareAndSwap(expect, update);
   }

   private CompletableFuture<Long> addAndCheckBound(CounterValue expected, long delta) {

      CounterValue updated = Utils.add(getConfiguration(), expected, delta);
      return compareAndSwap(expected.getValue(), updated.getValue())
            .thenCompose(swapValue -> {
               if (swapValue.equals(expected.getValue())) {
                  return checkResult(updated);
               } else {
                  return addAndCheckBound(CounterValue.newCounterValue(swapValue), delta);
               }
            });
   }

   private static CompletionStage<Long> checkResult(CounterValue result) {
      switch (result.getState()) {
         case VALID:
            return CompletableFuture.completedFuture(result.getValue());
         case LOWER_BOUND_REACHED:
            return CompletableFuture.failedFuture(log.counterOurOfBounds(LOWER_BOUND));
         case UPPER_BOUND_REACHED:
            return CompletableFuture.failedFuture(log.counterOurOfBounds(UPPER_BOUND));
         default:
            throw new IllegalStateException();
      }
   }
}
