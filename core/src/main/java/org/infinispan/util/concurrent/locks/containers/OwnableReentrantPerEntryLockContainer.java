package org.infinispan.util.concurrent.locks.containers;

import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.util.concurrent.locks.containers.wrappers.OwnableReentrantLockWrapper;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * A per-entry lock container for OwnableReentrantLocks
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class OwnableReentrantPerEntryLockContainer extends AbstractPerEntryLockContainer<OwnableReentrantLockWrapper> {

   private static final Log log = LogFactory.getLog(OwnableReentrantPerEntryLockContainer.class);

   @Override
   protected Log getLog() {
      return log;
   }

   public OwnableReentrantPerEntryLockContainer(int concurrencyLevel, Equivalence<Object> keyEquivalence) {
      super(concurrencyLevel, keyEquivalence);
   }

   @Override
   protected OwnableReentrantLockWrapper newLockWrapper() {
      return new OwnableReentrantLockWrapper();
   }
}
