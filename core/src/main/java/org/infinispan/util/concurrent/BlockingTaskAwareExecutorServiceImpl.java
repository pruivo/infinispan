package org.infinispan.util.concurrent;

import org.infinispan.util.TimeService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * A special executor service that accepts a {@code BlockingRunnable}. This special runnable gives hints about the code
 * to be running in order to avoiding put a runnable that will block the thread. In this way, only when the runnable
 * says that is ready, it is sent to the real executor service
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
public class BlockingTaskAwareExecutorServiceImpl extends AbstractExecutorService implements BlockingTaskAwareExecutorService {

   private static final Log log = LogFactory.getLog(BlockingTaskAwareExecutorServiceImpl.class);
   private final BlockingQueue<BlockingRunnable> blockedTasks;
   private final ExecutorService executorService;
   private final TimeService timeService;
   private final ControllerThread controllerThread;
   private volatile boolean shutdown;

   public BlockingTaskAwareExecutorServiceImpl(ExecutorService executorService, TimeService timeService) {
      this.blockedTasks = new LinkedBlockingQueue<>();
      this.executorService = executorService;
      this.timeService = timeService;
      this.shutdown = false;
      this.controllerThread = new ControllerThread();
      controllerThread.start();
   }

   @Override
   public final void execute(BlockingRunnable runnable) {
      if (shutdown) {
         throw new RejectedExecutionException("Executor Service is already shutdown");
      }
      if (runnable.isReady()) {
         doExecute(runnable);
      } else {
         blockedTasks.offer(runnable);
         controllerThread.checkForReadyTask();
      }
      if (log.isTraceEnabled()) {
         log.tracef("Added a new task: %s task(s) are waiting", blockedTasks.size());
      }
   }

   @Override
   public void shutdown() {
      shutdown = true;
   }

   @Override
   public List<Runnable> shutdownNow() {
      shutdown = true;
      List<Runnable> runnableList = new LinkedList<>();
      runnableList.addAll(executorService.shutdownNow());
      runnableList.addAll(blockedTasks);
      controllerThread.interrupt();
      return runnableList;
   }

   @Override
   public boolean isShutdown() {
      return shutdown;
   }

   @Override
   public boolean isTerminated() {
      return shutdown && blockedTasks.isEmpty() && executorService.isTerminated();
   }

   @Override
   public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
      final long endTime = timeService.expectedEndTime(timeout, unit);
      synchronized (blockedTasks) {
         long waitTime = timeService.remainingTime(endTime, TimeUnit.MILLISECONDS);
         while (!blockedTasks.isEmpty() && waitTime > 0) {
            wait(waitTime);
         }
      }
      return isTerminated();
   }

   @Override
   public final void checkForReadyTasks() {
      controllerThread.checkForReadyTask();
   }

   @Override
   public void execute(Runnable command) {
      if (shutdown) {
         throw new RejectedExecutionException("Executor Service is already shutdown");
      }
      executorService.execute(command);
   }

   private void doExecute(BlockingRunnable runnable) {
      try {
         executorService.execute(runnable);
      } catch (RejectedExecutionException rejected) {
         //put it back!
         blockedTasks.offer(runnable);
      }
   }

   private class ControllerThread extends Thread {

      private final LinkedList<BlockingRunnable> readList;
      private final Semaphore semaphore;
      private volatile boolean interrupted;

      public ControllerThread() {
         super("Controller-Thread");
         readList = new LinkedList<>();
         semaphore = new Semaphore(0);
      }

      public void checkForReadyTask() {
         semaphore.release();
      }

      @Override
      public void interrupt() {
         interrupted = true;
         super.interrupt();
      }

      @Override
      public void run() {
         while (!interrupted) {
            try {
               semaphore.acquire();
            } catch (InterruptedException e) {
               return;
            }
            semaphore.drainPermits();
            for (Iterator<BlockingRunnable> iterator = blockedTasks.iterator(); iterator.hasNext(); ) {
               BlockingRunnable runnable = iterator.next();
               if (runnable.isReady()) {
                  iterator.remove();
                  readList.add(runnable);
               }
            }

            if (log.isTraceEnabled()) {
               log.tracef("Tasks executed=%s, still pending=%s", readList.size(), blockedTasks.size());
            }

            while (!readList.isEmpty()) {
               doExecute(readList.pop());
            }
         }
      }
   }
}
