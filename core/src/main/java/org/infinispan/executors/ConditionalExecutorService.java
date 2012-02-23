package org.infinispan.executors;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Iterator;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * A special executor service that accepts a {@code ConditionalRunnable}. This special runnable gives hints about the
 * code to be running in order to avoiding put a runnable that will block the thread. In this way, only when the
 * runnable says that is ready, it is sent to the real executor service
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class ConditionalExecutorService {

   private static final Log log = LogFactory.getLog(ConditionalExecutorService.class);
   private volatile SchedulerThread schedulerThread;
   private final LinkedBlockingDeque<ConditionalRunnable> activeRunnables;
   private final int coreThread;
   private final int maxThread;
   private final long keepAliveTime;
   private final TimeUnit timeUnit;
   private final ThreadFactory threadFactory;
   private boolean shutdown;

   public ConditionalExecutorService(int coreThread, int maxThread, long keepAliveTime, TimeUnit timeUnit,
                                     ThreadFactory threadFactory, int queueSize) {
      this.coreThread = coreThread;
      this.maxThread = maxThread;
      this.keepAliveTime = keepAliveTime;
      this.timeUnit = timeUnit;
      this.threadFactory = threadFactory;
      this.activeRunnables = new LinkedBlockingDeque<ConditionalRunnable>(queueSize);
      this.shutdown = false;
   }

   public void execute(ConditionalRunnable runnable) throws Exception {
      initIfNeeded();
      activeRunnables.put(runnable);
      notifyRunnableAdded();
      if (log.isTraceEnabled()) {
         log.tracef("Added a new task: %s task are waiting", activeRunnables.size());
      }
   }

   public void shutdown() {
      shutdown = true;
      if (schedulerThread != null) {
         schedulerThread.interrupt();
         schedulerThread = null;
      }
   }

   private ThreadPoolExecutor createExecutorService() {
      return new ThreadPoolExecutor(coreThread, maxThread, keepAliveTime, timeUnit, new SynchronousQueue<Runnable>(),
                                    threadFactory, new ThreadPoolExecutor.CallerRunsPolicy());
   }

   private synchronized void initIfNeeded() {
      if (shutdown) {
         throw new RejectedExecutionException();
      }
      if (schedulerThread == null) {
         schedulerThread = new SchedulerThread();
         schedulerThread.start();
      }
   }

   private void notifyRunnableAdded() {
      synchronized (activeRunnables) {
         activeRunnables.notify();
      }
   }

   private class SchedulerThread extends Thread {

      private final ThreadPoolExecutor executorService;
      private volatile boolean running;

      public SchedulerThread() {
         super("Scheduler-" + System.identityHashCode(ConditionalExecutorService.this));
         this.executorService = createExecutorService();
      }

      @Override
      public void run() {
         running = true;
         while (running) {
            try {

               synchronized (activeRunnables) {
                  if (activeRunnables.isEmpty()) {
                     activeRunnables.wait();
                  }
               }

               int tasksExecuted = 0;

               for (Iterator<ConditionalRunnable> iterator = activeRunnables.iterator(); iterator.hasNext(); ) {
                  ConditionalRunnable runnable = iterator.next();
                  if (runnable.isReady()) {
                     iterator.remove();
                     executorService.execute(runnable);
                     tasksExecuted++;
                  }
               }

               if (log.isTraceEnabled() && tasksExecuted > 0) {
                  log.tracef("Tasks executed=%s, still active=%s", tasksExecuted, activeRunnables.size());
               }
            } catch (InterruptedException e) {
               break;
            } catch (Throwable throwable) {
               if (log.isTraceEnabled()) {
                  log.tracef(throwable, "Exception caught while executing task");
               } else {
                  log.warnf("Exception caught while executing task: %s", throwable.getLocalizedMessage());
               }

            }
         }
      }

      @Override
      public void interrupt() {
         running = false;
         super.interrupt();
         executorService.shutdownNow();
      }
   }
}
