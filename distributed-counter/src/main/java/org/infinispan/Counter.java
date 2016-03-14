package org.infinispan;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
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
   default void increment() {
      add(1);
   }


   /**
    * Atomically decrements the counter and returns the new value
    *
    * @return The new value
    */
   default void decrement() {
      add(-1);
   }


   /**
    * Atomically adds the given value to the current value.
    *
    * @param delta the value to add
    * @return the updated value
    */
   void add(long delta);

   /**
    * Resets the counter to its initial value.
    */
   void reset();

}
