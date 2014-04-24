package org.infinispan.util.concurrent.locks.containers.wrappers;

import net.jcip.annotations.GuardedBy;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.LinkedList;
import java.util.Queue;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
public abstract class BaseLockWrapper implements LockWrapper {

   private static final Log log = LogFactory.getLog(BaseLockWrapper.class);
   private static final boolean trace = log.isTraceEnabled();
   private final Queue<LockHolder> holderQueue;

   protected BaseLockWrapper() {
      holderQueue = new LinkedList<LockHolder>();
   }

   @Override
   public final LockHolder add(Object key, Object lockOwner) {
      synchronized (holderQueue) {
         for (LockHolder lockHolder : holderQueue) {
            if (lockHolder.getOwner().equals(lockHolder)) {
               if (trace) {
                  log.tracef("Adding existing request. Key=%s, LockOwner=%s, LockHolder=%s", key, lockOwner, lockHolder);
               }
               return lockHolder;
            }
         }
         LockHolder lockHolder = new LockHolder(lockOwner, key);
         holderQueue.add(lockHolder);
         holderQueue.peek().markReady();
         if (trace) {
            log.tracef("Adding new request. Key=%s, LockOwner=%s, LockHolder=%s", key, lockOwner, lockHolder);
         }
         return lockHolder;
      }
   }

   @Override
   public final void unlock(Object owner) {
      if (internalUnlock(owner)) {
         removeFirstIf(owner);
      }
   }

   @Override
   public final void remove(LockHolder lockHolder) {
      synchronized (holderQueue) {
         if (trace) {
            log.tracef("Removing %s", lockHolder);
         }
         holderQueue.remove(lockHolder);
         markFirstReadyIfNotEmpty();
      }
   }

   @Override
   public final boolean isEmpty() {
      synchronized (holderQueue) {
         return holderQueue.isEmpty();
      }
   }

   protected abstract boolean internalUnlock(Object owner);

   private void removeFirstIf(Object lockOwner) {
      synchronized (holderQueue) {
         if (!holderQueue.isEmpty() && holderQueue.peek().getOwner().equals(lockOwner)) {
            if (trace) {
               log.tracef("Removing first! %s", holderQueue.peek());
            }
            holderQueue.remove();
            markFirstReadyIfNotEmpty();
         }
      }
   }

   @GuardedBy(value = "holderQueue")
   private void markFirstReadyIfNotEmpty() {
      if (!holderQueue.isEmpty()) {
         holderQueue.peek().markReady();
      }
   }

}
