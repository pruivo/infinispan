package org.infinispan.commons.util;

import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public class Notifier<T> {

   private final Invoker<T> invoker;
   private final Queue<T> listeners;
   private final CountDownLatch waiter;
   private volatile boolean notify;

   public Notifier(Invoker<T> invoker) {
      Objects.requireNonNull(invoker, "Invoker must be non-null");
      this.invoker = invoker;
      this.listeners = new ConcurrentLinkedQueue<>();
      this.notify = false;
      this.waiter = new CountDownLatch(1);
   }

   public void add(T listener) {
      Objects.requireNonNull(listener, "Listener must be non-null");
      listeners.add(listener);
      if (notify) {
         trigger();
      }
   }

   public boolean await(long time, TimeUnit unit) throws InterruptedException {
      Objects.requireNonNull(unit, "TimeUnit must be non-null");
      return waiter.await(time, unit);
   }

   public void fireListener() {
      if (!notify) {
         //avoid CPU cache invalidation.
         this.notify = true;
         waiter.countDown();
         trigger();
      }
   }

   private void trigger() {
      T listener;
      while ((listener = listeners.poll()) != null) {
         invoker.invoke(listener);
      }
   }

   public interface Invoker<T1> {
      void invoke(T1 invoker);
   }
}
