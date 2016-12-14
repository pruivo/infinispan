package org.infinispan.executors;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.util.concurrent.BlockingRunnable;
import org.infinispan.util.concurrent.BlockingTaskAwareExecutorService;
import org.infinispan.util.concurrent.BlockingTaskAwareExecutorServiceImpl;
import org.testng.annotations.Test;

/**
 * Simple executor test
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
@Test(groups = "functional", testName = "executors.BlockingTaskAwareExecutorServiceTest")
public class BlockingTaskAwareExecutorServiceTest extends AbstractInfinispanTest {

   private static final AtomicInteger THREAD_ID = new AtomicInteger(0);

   public void testSimpleExecution() throws Exception {
      BlockingTaskAwareExecutorService executorService = createExecutorService(false);
      try {
         final DoSomething doSomething = new DoSomething();
         executorService.execute(doSomething);

         Thread.sleep(100);

         assertFalse(doSomething.isReady());
         assertFalse(doSomething.isExecuted());

         doSomething.markReady();
         assertTrue(doSomething.isReady());
         executorService.checkForReadyTasks();

         eventually(doSomething::isExecuted);
      } finally {
         executorService.shutdownNow();
      }
   }

   public void testMultipleExecutions() throws Exception {
      BlockingTaskAwareExecutorServiceImpl executorService = createExecutorService(false);
      try {
         List<DoSomething> tasks = new LinkedList<>();

         for (int i = 0; i < 30; ++i) {
            tasks.add(new DoSomething());
         }

         tasks.forEach(executorService::execute);

         for (DoSomething doSomething : tasks) {
            assert !doSomething.isReady();
            assert !doSomething.isExecuted();
         }

         tasks.forEach(BlockingTaskAwareExecutorServiceTest.DoSomething::markReady);
         executorService.checkForReadyTasks();

         for (final DoSomething doSomething : tasks) {
            eventually(doSomething::isExecuted);
         }

      } finally {
         executorService.shutdownNow();
      }
   }

   public void testExecuteOnSubmitThread() {
      BlockingTaskAwareExecutorService executorService = createExecutorService(true);
      DoOtherThing doOtherThing = new DoOtherThing();
      executorService.submit(doOtherThing);
      assertEquals(Thread.currentThread(), doOtherThing.executionThread);
   }

   private BlockingTaskAwareExecutorServiceImpl createExecutorService(boolean executeOnSubmitThread) {
      final String controllerName = "Controller-" + getClass().getSimpleName();
      final ExecutorService realOne = new ThreadPoolExecutor(1, 2, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<>(),
            new DummyThreadFactory());
      return new BlockingTaskAwareExecutorServiceImpl(controllerName, realOne, TIME_SERVICE, executeOnSubmitThread);
   }

   public static class DummyThreadFactory implements ThreadFactory {

      @Override
      public Thread newThread(Runnable runnable) {
         return new Thread(runnable, "Remote-" + getClass().getSimpleName() + "-" + THREAD_ID.incrementAndGet());
      }
   }

   public static class DoOtherThing implements BlockingRunnable {

      private volatile Thread executionThread;

      @Override
      public boolean isReady() {
         return true;
      }

      @Override
      public void run() {
         executionThread = Thread.currentThread();
      }
   }

   public static class DoSomething implements BlockingRunnable {

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

      synchronized final void markReady() {
         ready = true;
      }

      synchronized final boolean isExecuted() {
         return executed;
      }
   }
}
