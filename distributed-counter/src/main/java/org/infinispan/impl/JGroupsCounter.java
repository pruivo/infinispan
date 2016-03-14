package org.infinispan.impl;

import org.infinispan.AtomicCounter;
import org.jgroups.blocks.atomic.Counter;
import org.jgroups.blocks.atomic.CounterService;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public class JGroupsCounter implements AtomicCounter {

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
   public void increment() {
      counter.incrementAndGet();
   }

   @Override
   public void decrement() {
      counter.decrementAndGet();
   }

   @Override
   public void add(long delta) {
      counter.addAndGet(delta);
   }

   @Override
   public void set(long value) {
      counter.set(value);
   }

   @Override
   public boolean compareAndSet(long expect, long update) {
      return counter.compareAndSet(expect, update);
   }

   @Override
   public long incrementAndGet() {
      return counter.incrementAndGet();
   }

   @Override
   public long decrementAndGet() {
      return counter.decrementAndGet();
   }

   @Override
   public long addAndGet(long delta) {
      return counter.addAndGet(delta);
   }

   @Override
   public void reset() {
      set(initialValue);
   }
}
