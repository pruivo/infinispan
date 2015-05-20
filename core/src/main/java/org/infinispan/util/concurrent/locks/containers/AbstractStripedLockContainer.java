package org.infinispan.util.concurrent.locks.containers;

import net.jcip.annotations.ThreadSafe;
import org.infinispan.util.StripedHashFunction;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

/**
 * A container for locks.  Used with lock striping.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
@ThreadSafe
public abstract class AbstractStripedLockContainer<L extends Lock> extends AbstractLockContainer<L> {
   protected StripedHashFunction<Object> hashFunction;

   @Override
   public L acquireLock(Object lockOwner, Object key, long timeout, TimeUnit unit) throws InterruptedException {
      L lock = getLock(key);
      boolean locked;
      try {
         locked = tryLock(lock, timeout, unit, lockOwner);
      } catch (InterruptedException ie) {
         safeRelease(lock, lockOwner);
         throw ie;
      } catch (Throwable th) {
         safeRelease(lock, lockOwner);
         locked = false;
      }
      return locked ? lock : null;
   }

   @Override
   public void releaseLock(Object lockOwner, Object key) {
      final L lock = getLock(key);
      safeRelease(lock, lockOwner);
   }

   @Override
   public int getLockId(Object key) {
      return hashFunction.hashToSegment(key);
   }
}
