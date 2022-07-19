package org.infinispan.counter.impl.raft;

import java.lang.invoke.MethodHandles;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterListener;
import org.infinispan.counter.api.Handle;
import org.infinispan.counter.api.StrongCounter;
import org.infinispan.counter.api.SyncStrongCounter;
import org.infinispan.counter.impl.SyncStrongCounterAdapter;
import org.infinispan.counter.impl.entries.CounterValue;
import org.infinispan.counter.impl.manager.InternalCounterAdmin;
import org.infinispan.counter.impl.raft.operation.AddAndGetOperation;
import org.infinispan.counter.impl.raft.operation.CompareAndSwapOperation;
import org.infinispan.counter.impl.raft.operation.ResetOperation;
import org.infinispan.util.ByteString;

/**
 * TODO!
 */
public class RaftCounter implements StrongCounter, InternalCounterAdmin {

   public static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass(), Log.class);

   private final ByteString name;
   private final RaftOperationChannel raftChannel;
   private final CounterConfiguration configuration;
   private volatile CounterValue counterValue;

   RaftCounter(ByteString name, RaftOperationChannel raftChannel, CounterConfiguration configuration) {
      this.name = Objects.requireNonNull(name);
      this.raftChannel = Objects.requireNonNull(raftChannel);
      this.configuration = configuration;
      counterValue = CounterValue.newCounterValue(configuration);
   }

   @Override
   public String getName() {
      return name.toString();
   }

   @Override
   public CompletableFuture<Long> getValue() {
      return CompletableFuture.completedFuture(counterValue.getValue());
   }

   @Override
   public CompletableFuture<Long> addAndGet(long delta) {
      if (delta == 0) {
         return getValue();
      }
      AddAndGetOperation op = new AddAndGetOperation(name, delta);
      return raftChannel.sendOperation(op)
            .thenApply(op)
            .toCompletableFuture();
   }

   @Override
   public StrongCounter asStrongCounter() {
      return this;
   }

   @Override
   public CompletionStage<Void> destroy() {
      return null;
   }

   @Override
   public CompletableFuture<Void> reset() {
      ResetOperation op = new ResetOperation(name);
      return raftChannel.sendOperation(op)
            .thenApply(op)
            .toCompletableFuture();
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
      return null;
   }

   @Override
   public CompletableFuture<Long> compareAndSwap(long expect, long update) {
      CompareAndSwapOperation op = new CompareAndSwapOperation(name, expect, update);
      return raftChannel.sendOperation(op)
            .thenApply(op)
            .toCompletableFuture();
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

   public CounterValue get() {
      return counterValue;
   }

   public void set(CounterValue counterValue) {
      if (log.isTraceEnabled()) {
         log.tracef("Raft Counter '%s' updated: %s", name, counterValue);
      }
      this.counterValue = Objects.requireNonNull(counterValue);
   }
}
