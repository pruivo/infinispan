package org.infinispan.commons.util;

import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public class Notifier<T> {

   private final Invoker<T> invoker;
   private final Queue<T> listeners;
   private volatile boolean notify;

   public Notifier(Invoker<T> invoker) {
      Objects.requireNonNull(invoker, "Invoker must be non-null");
      this.invoker = invoker;
      this.listeners = new ConcurrentLinkedQueue<>();
      this.notify = false;
   }

   public void add(T listener) {
      listeners.add(listener);
      if (notify) {
         trigger();
      }
   }

   public void fireListener() {
      this.notify = true;
      trigger();
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
