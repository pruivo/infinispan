package org.infinispan.util.concurrent.locks.order;

import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.commons.util.concurrent.jdk8backported.EquivalentConcurrentHashMapV8;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.util.concurrent.BlockingTaskAwareExecutorService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ExecutorService;

import static org.infinispan.factories.KnownComponentNames.REMOTE_COMMAND_EXECUTOR;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class PerEntryRemoteLockOrderManager extends BaseRemoteLockOrderManager<KeyBasedLockLatch> {

   /*
    * TODO LIST:
    * handle topology change: if we are not the primary owner, release the latches.
    *
    */

   private static final Log log = LogFactory.getLog(PerEntryRemoteLockOrderManager.class);
   private static final boolean trace = log.isTraceEnabled();

   private EquivalentConcurrentHashMapV8<Object, Queue<KeyBasedLockLatch>> queueMap;

   @Inject
   public void init(Configuration configuration, ClusteringDependentLogic clusteringDependentLogic,
                    @ComponentName(REMOTE_COMMAND_EXECUTOR) ExecutorService executorService) {
      if (!(executorService instanceof BlockingTaskAwareExecutorService)) {
         throw new IllegalArgumentException("Wrong executor type. expects BlockingTaskAwareExecutorService but was" + executorService.getClass());
      }
      initialize(clusteringDependentLogic, (BlockingTaskAwareExecutorService) executorService);
      queueMap = new EquivalentConcurrentHashMapV8<>(configuration.dataContainer().keyEquivalence(), AnyEquivalence.getInstance());
   }

   public boolean isEmpty() {
      return queueMap.isEmpty();
   }

   @Override
   protected synchronized LockLatch orderMultipleKeys(Collection<Object> keys) {
      Collection<KeyBasedLockLatch> latchCollection = new ArrayList<>(keys.size());
      for (Object key : keys) {
         if (isLockOwner(key)) {
            InsertFunction function = new InsertFunction();
            queueMap.compute(key, function);
            latchCollection.add(function.lockLatch);
         }
      }
      return latchCollection.isEmpty() ? LockLatch.NO_OP : new CompositeLockLatch(latchCollection);
   }

   @Override
   protected LockLatch orderSingleKey(Object key) {
      if (!isLockOwner(key)) {
         return LockLatch.NO_OP;
      }
      InsertFunction function = new InsertFunction();
      queueMap.compute(key, function);
      return function.lockLatch;
   }

   @Override
   protected Log getLog() {
      return log;
   }

   @Override
   protected boolean isTraceEnabled() {
      return trace;
   }

   @Override
   protected boolean releaseLatch(KeyBasedLockLatch lockLatch) {
      ReleaseFunction function = new ReleaseFunction(lockLatch);
      queueMap.compute(lockLatch.getKey(), function);
      return function.notify;
   }

   private class InsertFunction implements EquivalentConcurrentHashMapV8.BiFun<Object, Queue<KeyBasedLockLatch>, Queue<KeyBasedLockLatch>> {

      private KeyBasedLockLatch lockLatch;

      @Override
      public Queue<KeyBasedLockLatch> apply(Object key, Queue<KeyBasedLockLatch> queue) {
         if (queue == null) {
            queue = new LinkedList<>();
         }
         this.lockLatch = new KeyBasedLockLatch(key, PerEntryRemoteLockOrderManager.this);
         doInsert(queue, this.lockLatch);
         return queue;
      }
   }

   private class ReleaseFunction implements EquivalentConcurrentHashMapV8.BiFun<Object, Queue<KeyBasedLockLatch>, Queue<KeyBasedLockLatch>> {

      private final KeyBasedLockLatch lockLatch;
      private boolean notify;

      private ReleaseFunction(KeyBasedLockLatch lockLatch) {
         this.lockLatch = lockLatch;
         this.notify = false;
      }

      @Override
      public Queue<KeyBasedLockLatch> apply(Object key, Queue<KeyBasedLockLatch> queue) {
         if (queue == null) {
            return null;
         }
         this.notify = doRelease(queue, lockLatch);
         return queue.isEmpty() ? null : queue;
      }
   }
}
