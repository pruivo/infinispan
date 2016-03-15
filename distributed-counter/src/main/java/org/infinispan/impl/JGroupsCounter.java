package org.infinispan.impl;

import org.jgroups.blocks.atomic.Counter;
import org.jgroups.blocks.atomic.CounterService;

import java.util.concurrent.CompletableFuture;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public class JGroupsCounter implements org.infinispan.Counter {

   private final Counter counter;
   private final long initialValue;

   public JGroupsCounter(CounterService service, String name, long initialValue) {
      this.counter = service.getOrCreateCounter(name, initialValue);
      this.initialValue = initialValue;
   }

   @Override
   public String getName() {
      return counter.getName();
   }

   @Override
   public long get() {
      return counter.get();
   }

   @Override
   public CompletableFuture<Long> addAndGet(long delta) {
      return CompletableFuture.completedFuture(counter.addAndGet(delta));
   }

   @Override
   public void reset() {
      counter.set(initialValue);
   }
}
