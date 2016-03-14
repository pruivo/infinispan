package org.infinispan;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public interface AtomicCounter extends Counter {


   /**
    * Sets the counter to a new value
    *
    * @param value The new value
    */
   void set(long value);


   /**
    * Atomically updates the counter using a CAS operation
    *
    * @param expect The expected value of the counter
    * @param update The new value of the counter
    * @return {@code true} if the counter could be updated, {@code false} otherwise
    */
   boolean compareAndSet(long expect, long update);


   /**
    * Atomically increments the counter and returns the new value
    *
    * @return The new value
    */
   default long incrementAndGet() {
      return addAndGet(1);
   }


   /**
    * Atomically decrements the counter and returns the new value
    *
    * @return The new value
    */
   default long decrementAndGet() {
      return addAndGet(-1);
   }


   /**
    * Atomically adds the given value to the current value.
    *
    * @param delta the value to add
    * @return the updated value
    */
   long addAndGet(long delta);

}
