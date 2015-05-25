package org.infinispan.util.concurrent.locks.order;

import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.util.concurrent.BlockingTaskAwareExecutorService;
import org.infinispan.util.logging.Log;

import java.util.Collection;
import java.util.Queue;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public abstract class BaseRemoteLockOrderManager<L extends ExtendedLockLatch> implements RemoteLockOrderManager {

   private ClusteringDependentLogic clusteringDependentLogic;
   private BlockingTaskAwareExecutorService executorService;

   @Override
   public final LockLatch order(RemoteLockCommand command) {
      Collection<Object> keysToLock = command.getKeysToLock();
      if (isTraceEnabled()) {
         getLog().tracef("Ordering %s.", String.valueOf(keysToLock));
      }
      if (keysToLock.isEmpty()) {
         return LockLatch.NO_OP;
      } else if (keysToLock.size() > 1) {
         return orderMultipleKeys(keysToLock);
      } else {
         return orderSingleKey(keysToLock.iterator().next());
      }
   }

   protected abstract LockLatch orderMultipleKeys(Collection<Object> keys);

   protected abstract LockLatch orderSingleKey(Object key);

   protected abstract Log getLog();

   protected abstract boolean isTraceEnabled();

   protected final void initialize(ClusteringDependentLogic clusteringDependentLogic,
                                   BlockingTaskAwareExecutorService executorService) {
      this.clusteringDependentLogic = clusteringDependentLogic;
      this.executorService = executorService;
   }

   protected final void doInsert(Queue<L> queue, L latch) {
      if (isTraceEnabled()) {
         getLog().tracef("Inserting latch %s in queue. Is first? %s", latch, queue.isEmpty());
      }
      if (queue.isEmpty()) {
         latch.markReady();
      }
      queue.add(latch);
   }

   protected final boolean doRelease(Queue<L> queue, L latch) {
      if (queue.peek() == latch) {
         queue.poll();
         if (queue.isEmpty()) {
            if(isTraceEnabled()) {
               getLog().tracef("Releasing latch %s. No more latches waiting.", latch);
            }
            return false;
         }
         queue.peek().markReady();
         if(isTraceEnabled()) {
            getLog().tracef("Releasing latch %s. Mark ready %s.", latch, queue.peek());
         }
         return true;
      } else {
         if(isTraceEnabled()) {
            getLog().tracef("Releasing latch %s failed! It is not the first in queue. First is %s.", latch, queue.peek());
         }
      }
      return false;
   }

   protected final boolean isLockOwner(Object key) {
      return clusteringDependentLogic.localNodeIsPrimaryOwner(key);
   }

   protected abstract boolean releaseLatch(L lockLatch);

   protected void release(L lockLatch) {
      if (isTraceEnabled()) {
         getLog().tracef("About to release latch %s.", lockLatch);
      }
      if (releaseLatch(lockLatch)) {
         notifyExecutor();
      }
   }

   private void notifyExecutor() {
      if (isTraceEnabled()) {
         getLog().tracef("Notifying executor service!");
      }
      executorService.checkForReadyTasks();
   }

   private void release(Collection<L> latchCollection) {
      if (isTraceEnabled()) {
         getLog().tracef("About to release latch %s.", latchCollection);
      }
      boolean notify = false;
      for (L lockLatch : latchCollection) {
         notify = releaseLatch(lockLatch) || notify;
      }
      if (notify) {
         notifyExecutor();
      }
   }

   public class CompositeLockLatch implements LockLatch {

      private final Collection<L> latchCollection;

      public CompositeLockLatch(Collection<L> latchCollection) {
         this.latchCollection = latchCollection;
      }

      @Override
      public boolean isReady() {
         for (L latch : latchCollection) {
            if (!latch.isReady()) {
               return false;
            }
         }
         return true;
      }

      @Override
      public void release() {
         BaseRemoteLockOrderManager.this.release(latchCollection);
      }

      @Override
      public String toString() {
         return "CompositeLockLatch{" +
               "latchCollection=" + latchCollection +
               ", id=" + hashCode() +
               '}';
      }
   }

}
