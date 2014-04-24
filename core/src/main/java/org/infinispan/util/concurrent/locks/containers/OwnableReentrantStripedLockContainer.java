package org.infinispan.util.concurrent.locks.containers;

import net.jcip.annotations.ThreadSafe;
import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.util.concurrent.locks.containers.wrappers.LockWrapper;
import org.infinispan.util.concurrent.locks.containers.wrappers.OwnableReentrantLockWrapper;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Arrays;

/**
 * A LockContainer that holds {@link org.infinispan.util.concurrent.locks.OwnableReentrantLock}s.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @see ReentrantStripedLockContainer
 * @see org.infinispan.util.concurrent.locks.OwnableReentrantLock
 * @since 4.0
 */
@ThreadSafe
public class OwnableReentrantStripedLockContainer extends AbstractStripedLockContainer<OwnableReentrantLockWrapper> {

   private final OwnableReentrantLockWrapper[] sharedLocks;
   private static final Log log = LogFactory.getLog(OwnableReentrantStripedLockContainer.class);

   /**
    * Creates a new LockContainer which uses a certain number of shared locks across all elements that need to be
    * locked.
    *
    * @param concurrencyLevel concurrency level for number of stripes to create.  Stripes are created in powers of two,
    *                         with a minimum of concurrencyLevel created.
    */
   public OwnableReentrantStripedLockContainer(int concurrencyLevel, Equivalence<Object> keyEquivalence) {
      super(keyEquivalence);
      int numLocks = calculateNumberOfSegments(concurrencyLevel);
      sharedLocks = new OwnableReentrantLockWrapper[numLocks];
      for (int i = 0; i < numLocks; i++) sharedLocks[i] = new OwnableReentrantLockWrapper();
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
   public final OwnableReentrantLockWrapper getLock(Object object) {
      return sharedLocks[hashToIndex(object)];
   }

   @Override
   public final int getNumLocksHeld() {
      int i = 0;
      for (OwnableReentrantLockWrapper l : sharedLocks) if (l.isLocked()) i++;
      return i;
   }

   @Override
   public String toString() {
      return "OwnableReentrantStripedLockContainer{" +
            "sharedLocks=" + Arrays.toString(sharedLocks) +
            '}';
   }

   @Override
   public int size() {
      return sharedLocks.length;
   }

   @Override
   protected Log getLog() {
      return log;
   }
}
