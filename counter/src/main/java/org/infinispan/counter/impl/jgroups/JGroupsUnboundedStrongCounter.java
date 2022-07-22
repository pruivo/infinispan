package org.infinispan.counter.impl.jgroups;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterListener;
import org.infinispan.counter.api.Handle;
import org.infinispan.counter.api.StrongCounter;
import org.infinispan.counter.api.SyncStrongCounter;
import org.infinispan.counter.impl.SyncStrongCounterAdapter;
import org.infinispan.counter.impl.manager.InternalCounterAdmin;
import org.jgroups.blocks.atomic.AsyncCounter;

/**
 * TODO! document this
 */
public class JGroupsUnboundedStrongCounter implements StrongCounter, InternalCounterAdmin {

   private final String name;
   private final CounterConfiguration configuration;
   private final AsyncCounter counter;

   public JGroupsUnboundedStrongCounter(String name, CounterConfiguration configuration, AsyncCounter counter) {
      this.name = name;
      this.configuration = configuration;
      this.counter = counter;
   }

   @Override
   public String getName() {
      return name;
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
      return reset();
   }

   @Override
   public CompletableFuture<Void> reset() {
      return counter.set(configuration.initialValue()).toCompletableFuture();
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
   public <T extends CounterListener> Handle<T> addListener(T listener) {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletableFuture<Long> compareAndSwap(long expect, long update) {
      return counter.compareAndSwap(expect, update).toCompletableFuture();
   }

   @Override
   public CounterConfiguration getConfiguration() {
      return configuration;
   }

   @Override
   public CompletableFuture<Void> remove() {
      return reset();
   }

   @Override
   public SyncStrongCounter sync() {
      return new SyncStrongCounterAdapter(this);
   }

   @Override
   public StrongCounter asStrongCounter() {
      return this;
   }
}
