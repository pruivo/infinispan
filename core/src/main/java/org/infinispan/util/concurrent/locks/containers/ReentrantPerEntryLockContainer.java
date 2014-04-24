package org.infinispan.util.concurrent.locks.containers;

import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.util.concurrent.locks.containers.wrappers.ReentrantLockWrapper;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * A per-entry lock container for ReentrantLocks
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class ReentrantPerEntryLockContainer extends AbstractPerEntryLockContainer<ReentrantLockWrapper> {

   private static final Log log = LogFactory.getLog(ReentrantPerEntryLockContainer.class);

   @Override
   protected Log getLog() {
      return log;
   }

   public ReentrantPerEntryLockContainer(int concurrencyLevel, Equivalence<Object> keyEquivalence) {
      super(concurrencyLevel, keyEquivalence);
   }

   @Override
   protected ReentrantLockWrapper newLockWrapper() {
      return new ReentrantLockWrapper();
   }
}
