package org.infinispan.executors;

/**
 * A special Runnable (for the particular case of Total Order) that is only set to a thread when it is ready to
 * be executed without blocking the thread
 *
 * Use case:
 *    - in Total Order, when the prepare is delivered, the runnable blocks waiting for the previous conflicting
 *    transactions to be finished. In a normal executor service, this will take a thread and that thread will be
 *    blocked. This way, the runnable waits on the queue and not in the Thread
 *
 * Used in {@code ConditionalExecutorService}
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public interface ConditionalRunnable extends Runnable {

   /**
    * @return  true if this Runnable is ready to be executed without blocking
    */
   boolean isReady();

}
