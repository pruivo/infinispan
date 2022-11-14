package org.infinispan.counter.impl.jgroups;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterListener;
import org.infinispan.counter.api.Handle;
import org.infinispan.counter.api.StrongCounter;
import org.infinispan.counter.api.SyncStrongCounter;
import org.infinispan.counter.impl.SyncStrongCounterAdapter;
import org.infinispan.counter.impl.manager.InternalCounterAdmin;
import org.jgroups.blocks.atomic.AsyncCounter;

/**
 * //TODO document this!
 *
 * @author Pedro Ruivo
 * @since 15.0
 */
public class UnboundedJGroupsStrongCounter implements StrongCounter, InternalCounterAdmin {

   private final AsyncCounter counter;
   private final CounterConfiguration counterConfiguration;
   private final Function<String, CompletionStage<Void>> removeFunction;

   public UnboundedJGroupsStrongCounter(AsyncCounter counter, CounterConfiguration counterConfiguration, Function<String, CompletionStage<Void>> removeFunction) {
      this.counter = Objects.requireNonNull(counter);
      this.counterConfiguration = Objects.requireNonNull(counterConfiguration);
      this.removeFunction = Objects.requireNonNull(removeFunction);
   }

   @Override
   public String getName() {
      return counter.getName();
   }

   @Override
   public CompletableFuture<Long> getValue() {
      return counter.get().toCompletableFuture();
   }

   @Override
   public CompletableFuture<Long> addAndGet(long delta) {
      return counter.addAndGet(delta).toCompletableFuture();
   }

   @Override
   public CompletionStage<Void> destroy() {
      return null;
   }

   @Override
   public CompletableFuture<Void> reset() {
      return counter.set(counterConfiguration.initialValue()).toCompletableFuture();
   }

   @Override
   public CompletionStage<Long> value() {
      return getValue();
   }

   @Override
   public boolean isWeakCounter() {
      return false;
   }

   @Override
   public StrongCounter asStrongCounter() {
      return this;
   }

   @Override
   public <T extends CounterListener> Handle<T> addListener(T listener) {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletableFuture<Long> compareAndSwap(long expect, long update) {
      return counter.compareAndSwap(expect, update).toCompletableFuture();
   }

   @Override
   public CounterConfiguration getConfiguration() {
      return counterConfiguration;
   }

   @Override
   public CompletableFuture<Void> remove() {
      return removeFunction.apply(getName()).toCompletableFuture();
   }

   @Override
   public SyncStrongCounter sync() {
      return new SyncStrongCounterAdapter(this);
   }
}
