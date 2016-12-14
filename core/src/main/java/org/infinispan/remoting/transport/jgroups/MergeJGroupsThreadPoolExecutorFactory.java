package org.infinispan.remoting.transport.jgroups;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;

import org.infinispan.commons.executors.BlockingThreadPoolExecutorFactory;
import org.jgroups.util.DirectExecutor;

/**
 * A thread pool factory that returns the JGroups' thread pool if available.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class MergeJGroupsThreadPoolExecutorFactory extends BlockingThreadPoolExecutorFactory {

   public static final String MERGE_THREAD_POOL_NAME = "_jgroups_thread_pool_";
   private final JGroupsTransport jGroupsTransport;

   public MergeJGroupsThreadPoolExecutorFactory(JGroupsTransport transport, int maxThreads, int coreThreads,
         int queueLength, long keepAlive) {
      super(maxThreads, coreThreads, queueLength, keepAlive);
      this.jGroupsTransport = transport;
   }

   private static ExecutorService extractExecutorService(JGroupsTransport transport) {
      Executor executor = transport.getChannel().getProtocolStack().getTransport().getThreadPool();
      return executor instanceof ExecutorService && !(executor instanceof DirectExecutor) ?
            (ExecutorService) executor : null;
   }

   @Override
   public ExecutorService createExecutor(ThreadFactory factory) {
      ExecutorService executorService = extractExecutorService(jGroupsTransport);
      return executorService == null ?
            super.createExecutor(factory) :
            executorService;
   }

   @Override
   public void validate() {
      super.validate();
   }
}
