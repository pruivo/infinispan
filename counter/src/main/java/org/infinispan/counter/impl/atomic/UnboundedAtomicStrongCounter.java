package org.infinispan.counter.impl.atomic;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.LongFunction;

import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterEvent;
import org.infinispan.counter.api.CounterListener;
import org.infinispan.counter.api.CounterState;
import org.infinispan.counter.api.Handle;
import org.infinispan.counter.api.StrongCounter;
import org.infinispan.counter.api.SyncStrongCounter;
import org.infinispan.counter.impl.listener.CounterEventImpl;
import org.infinispan.counter.impl.listener.CounterManagerNotificationManager;

import net.jcip.annotations.GuardedBy;

/**
 * //TODO document this!
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class UnboundedAtomicStrongCounter implements StrongCounter {

   final SyncStrongCounter counter;
   final CounterManagerNotificationManager notificationManager;
   private final CopyOnWriteArrayList<ListenerHandler<?>> listeners = new CopyOnWriteArrayList<>();

   public UnboundedAtomicStrongCounter(String name, CounterConfiguration configuration,
                                       CounterManagerNotificationManager notificationManager) {
      this(name, configuration, notificationManager, ValidCounterHolder::new);
   }

   UnboundedAtomicStrongCounter(String name, CounterConfiguration configuration,
                                CounterManagerNotificationManager notificationManager, LongFunction<CounterHolder> builder) {
      this.notificationManager = notificationManager;
      this.counter = new Sync(configuration, name, builder);
   }


   @Override
   public String getName() {
      return counter.getName();
   }

   @Override
   public CompletableFuture<Long> getValue() {
      return CompletableFuture.completedFuture(counter.getValue());
   }

   @Override
   public CompletableFuture<Long> addAndGet(long delta) {
      try {
         return CompletableFuture.completedFuture(counter.addAndGet(delta));
      } catch (Throwable t) {
         return CompletableFutures.completedExceptionFuture(t);
      }
   }

   @Override
   public CompletableFuture<Void> reset() {
      try {
         counter.reset();
         return CompletableFutures.completedNull();
      } catch (Throwable t) {
         return CompletableFutures.completedExceptionFuture(t);
      }
   }

   @Override
   public <T extends CounterListener> Handle<T> addListener(T listener) {
      ListenerHandler<T> handler = new ListenerHandler<>(listener);
      listeners.add(handler);
      return handler;
   }

   @Override
   public CompletableFuture<Long> compareAndSwap(long expect, long update) {
      try {
         return CompletableFuture.completedFuture(counter.compareAndSwap(expect, update));
      } catch (Throwable t) {
         return CompletableFutures.completedExceptionFuture(t);
      }
   }

   @Override
   public CounterConfiguration getConfiguration() {
      return counter.getConfiguration();
   }

   @Override
   public CompletableFuture<Void> remove() {
      return reset();
   }

   @Override
   public SyncStrongCounter sync() {
      return counter;
   }

   void triggerEvent(CounterEvent event) {
      notificationManager.triggerEvent(listeners, event);
   }

   long add(CounterHolder data, long delta) {
      long newValue;
      try {
         newValue = Math.addExact(data.getValue(), delta);
      } catch (ArithmeticException e) {
         //overflow!
         newValue = delta > 0 ? Long.MAX_VALUE : Long.MIN_VALUE;
      }
      triggerEvent(CounterEventImpl.create(data.getValue(), newValue));
      data.setValue(newValue);
      return newValue;
   }

   long cas(CounterHolder data, long expect, long update) {
      long value = data.getValue();
      if (value != expect) {
         return value;
      }
      triggerEvent(CounterEventImpl.create(expect, update));
      data.setValue(update);
      return expect;
   }

   private class Sync implements SyncStrongCounter {

      private final CounterConfiguration configuration;
      private final String name;
      @GuardedBy("this")
      private CounterHolder data;


      private Sync(CounterConfiguration configuration, String name, LongFunction<CounterHolder> builder) {
         this.configuration = configuration;
         this.name = name;
         this.data = builder.apply(configuration.initialValue());
      }

      @Override
      public synchronized long addAndGet(long delta) {
         return add(data, delta);
      }

      @Override
      public synchronized void reset() {
         long oldValue = data.getValue();
         if (oldValue == configuration.initialValue()) {
            return;
         }
         CounterState oldState = data.getState();
         data.setValue(configuration.initialValue()).setState(CounterState.VALID);
         triggerEvent(CounterEventImpl.create(oldValue, oldState, data.getValue(), data.getState()));
      }

      @Override
      public synchronized long getValue() {
         return data.getValue();
      }

      @Override
      public synchronized long compareAndSwap(long expect, long update) {
         return cas(data, expect, update);
      }

      @Override
      public String getName() {
         return name;
      }

      @Override
      public CounterConfiguration getConfiguration() {
         return configuration;
      }

      @Override
      public void remove() {
         reset();
      }
   }

   private class ListenerHandler<L extends CounterListener> implements Handle<L>, CounterListener {

      private final L listener;

      private ListenerHandler(L listener) {
         this.listener = listener;
      }

      @Override
      public L getCounterListener() {
         return listener;
      }

      @Override
      public void remove() {
         listeners.remove(this);
      }

      @Override
      public void onUpdate(CounterEvent entry) {
         try {
            listener.onUpdate(entry);
         } catch (Throwable t) {
            //ignore
         }
      }
   }
}
