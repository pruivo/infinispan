package org.infinispan.executors;

import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", testName = "executors.ConditionalExecutorServiceTest")
public class ConditionalExecutorServiceTest extends AbstractInfinispanTest {

   public void simpleTest() throws Exception {
      ConditionalExecutorService executorService = createExecutorService();
      try {
         final DoSomething doSomething = new DoSomething();
         executorService.execute(doSomething);

         Thread.sleep(100);

         assert !doSomething.isReady();
         assert !doSomething.isExecuted();

         doSomething.markReady();

         assert doSomething.isReady();

         eventually(new Condition() {
            @Override
            public boolean isSatisfied() throws Exception {
               return doSomething.isExecuted();
            }
         });
      } finally {
         executorService.shutdown();
      }
   }

   public void simpleTest2() throws Exception {
      ConditionalExecutorService executorService = createExecutorService();
      try {
         List<DoSomething> tasks = new LinkedList<DoSomething>();

         for (int i = 0; i < 30; ++i) {
            tasks.add(new DoSomething());
         }

         for (DoSomething doSomething : tasks) {
            executorService.execute(doSomething);
         }

         for (DoSomething doSomething : tasks) {
            assert !doSomething.isReady();
            assert !doSomething.isExecuted();
         }

         for (DoSomething doSomething : tasks) {
            doSomething.markReady();
         }

         for (final DoSomething doSomething : tasks) {
            eventually(new Condition() {
               @Override
               public boolean isSatisfied() throws Exception {
                  return doSomething.isExecuted();
               }
            });
         }

      } finally {
         executorService.shutdown();
      }
   }

   private ConditionalExecutorService createExecutorService() {
      ConditionalExecutorService executorService = new ConditionalExecutorService(1, 2, 60, TimeUnit.SECONDS,
            new DummyThreadFactory(), 1000);
      return executorService;
   }

   public static class DummyThreadFactory implements ThreadFactory {

      @Override
      public Thread newThread(Runnable runnable) {
         return new Thread(runnable);
      }
   }

   public static class DoSomething implements ConditionalRunnable {

      private volatile boolean ready = false;
      private volatile boolean executed = false;

      @Override
      public synchronized final boolean isReady() {
         return ready;
      }

      @Override
      public synchronized final void run() {
         executed = true;
      }

      public synchronized final void markReady() {
         ready = true;
      }

      public synchronized final boolean isExecuted() {
         return executed;
      }
   }
}