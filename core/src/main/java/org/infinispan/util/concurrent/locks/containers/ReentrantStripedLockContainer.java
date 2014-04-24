package org.infinispan.util.concurrent.locks.containers;

import net.jcip.annotations.ThreadSafe;
import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.util.concurrent.locks.containers.wrappers.LockWrapper;
import org.infinispan.util.concurrent.locks.containers.wrappers.ReentrantLockWrapper;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Arrays;

/**
 * A LockContainer that holds ReentrantLocks
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @see OwnableReentrantStripedLockContainer
 * @since 4.0
 */
@ThreadSafe
public class ReentrantStripedLockContainer extends AbstractStripedLockContainer<ReentrantLockWrapper> {

   private final ReentrantLockWrapper[] sharedLocks;
   private static final Log log = LogFactory.getLog(ReentrantStripedLockContainer.class);

   /**
    * Creates a new LockContainer which uses a certain number of shared locks across all elements that need to be
    * locked.
    *
    * @param concurrencyLevel concurrency level for number of stripes to create.  Stripes are created in powers of two,
    *                         with a minimum of concurrencyLevel created.
    */
   public ReentrantStripedLockContainer(int concurrencyLevel, Equivalence<Object> keyEquivalence) {
      super(keyEquivalence);
      int numLocks = calculateNumberOfSegments(concurrencyLevel);
      sharedLocks = new ReentrantLockWrapper[numLocks];
      for (int i = 0; i < numLocks; i++) sharedLocks[i] = new ReentrantLockWrapper();
   }

   @Override
   public final ReentrantLockWrapper getLock(Object object) {
      return sharedLocks[hashToIndex(object)];
   }

   @Override
   public boolean isQueuesEmpty() {
      for (LockWrapper lockWrapper : sharedLocks) {
         if (!lockWrapper.isEmpty()) {
            return false;
         }
      }
      return true;
   }

   @Override
   public final int getNumLocksHeld() {
      int i = 0;
      for (ReentrantLockWrapper l : sharedLocks)
         if (l.isLocked()) {
            i++;
         }
      return i;
   }

   @Override
   public int size() {
      return sharedLocks.length;
   }

   @Override
   public String toString() {
      return "ReentrantStripedLockContainer{" +
            "sharedLocks=" + Arrays.toString(sharedLocks) +
            '}';
   }

   @Override
   protected Log getLog() {
      return log;
   }
}
