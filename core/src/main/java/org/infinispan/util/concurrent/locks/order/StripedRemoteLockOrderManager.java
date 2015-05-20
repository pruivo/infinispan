package org.infinispan.util.concurrent.locks.order;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.util.StripedHashFunction;
import org.infinispan.util.concurrent.BlockingTaskAwareExecutorService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ExecutorService;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class StripedRemoteLockOrderManager extends BaseRemoteLockOrderManager<IndexBasedLockLatch> {

   private static final Log log = LogFactory.getLog(StripedRemoteLockOrderManager.class);
   private static final boolean trace = log.isTraceEnabled();

   private StripedHashFunction<Object> hashFunction;
   private Queue<IndexBasedLockLatch>[] queues;

   @Inject
   public void init(Configuration configuration, ClusteringDependentLogic clusteringDependentLogic,
                    @ComponentName(value = KnownComponentNames.REMOTE_COMMAND_EXECUTOR) ExecutorService executorService) {
      if (!(executorService instanceof BlockingTaskAwareExecutorService)) {
         throw new IllegalArgumentException("Wrong executor type. expects BlockingTaskAwareExecutorService but was" + executorService.getClass());
      }
      initialize(clusteringDependentLogic, (BlockingTaskAwareExecutorService) executorService);
      this.hashFunction = StripedHashFunction.buildFromConfiguration(configuration);
      int segments = hashFunction.getNumSegments();
      //noinspection unchecked
      queues = new Queue[segments];
      for (int i = 0; i < segments; ++i) {
         queues[i] = new LinkedList<>();
      }
   }

   public boolean isEmpty() {
      for (Queue<?> queue : queues) {
         //noinspection SynchronizationOnLocalVariableOrMethodParameter
         synchronized (queue) {
            if (!queue.isEmpty()) {
               return false;
            }
         }
      }
      return true;
   }

   @Override
   protected synchronized LockLatch orderMultipleKeys(Collection<Object> keys) {
      BitSet bitSet = new BitSet(queues.length);
      for (Object key : keys) {
         if (isLockOwner(key)) {
            bitSet.set(hashFunction.hashToSegment(key));
         }
      }
      if (bitSet.isEmpty()) {
         return LockLatch.NO_OP;
      }
      Collection<IndexBasedLockLatch> latchCollection = new ArrayList<>(bitSet.cardinality());
      //check javadoc. this way, we can iterate over the true bits
      for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
         IndexBasedLockLatch lockLatch = new IndexBasedLockLatch(i, this);
         latchCollection.add(lockLatch);
         Queue<IndexBasedLockLatch> queue = queues[i];
         //noinspection SynchronizationOnLocalVariableOrMethodParameter
         synchronized (queue) {
            doInsert(queue, lockLatch);
         }
      }
      return new CompositeLockLatch(latchCollection);
   }

   @Override
   protected LockLatch orderSingleKey(Object key) {
      if (!isLockOwner(key)) {
         return LockLatch.NO_OP;
      }
      int index = hashFunction.hashToSegment(key);
      Queue<IndexBasedLockLatch> queue = queues[index];
      IndexBasedLockLatch lockLatch = new IndexBasedLockLatch(index, this);
      //noinspection SynchronizationOnLocalVariableOrMethodParameter
      synchronized (queue) {
         doInsert(queue, lockLatch);
      }
      return lockLatch;
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
   protected boolean releaseLatch(IndexBasedLockLatch lockLatch) {
      Queue<IndexBasedLockLatch> queue = queues[lockLatch.getIndex()];
      //noinspection SynchronizationOnLocalVariableOrMethodParameter
      synchronized (queue) {
         return doRelease(queue, lockLatch);
      }
   }
}
