package org.infinispan;

import java.util.concurrent.CompletableFuture;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public interface Counter {

   /**
    * @return the counter name.
    */
   String getName();


   /**
    * @return The current value
    */
   long get();


   /**
    * Atomically increments the counter and returns the new value
    *
    * @return The new value
    */
   default CompletableFuture<Long> incrementAndGet() {
      return addAndGet(1L);
   }


   /**
    * Atomically decrements the counter and returns the new value
    *
    * @return The new value
    */
   default CompletableFuture<Long> decrementAndGet() {
      return addAndGet(-1L);
   }


   /**
    * Atomically adds the given value to the current value.
    *
    * @param delta the value to add
    * @return the updated value
    */
   CompletableFuture<Long> addAndGet(long delta);

   /**
    * Resets the counter to its initial value.
    */
   void reset();

}
