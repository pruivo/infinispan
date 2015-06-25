package org.infinispan.factories;

import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.util.concurrent.locks.DeadlockDetectingLockManager;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.concurrent.locks.PendingLockManager;
import org.infinispan.util.concurrent.locks.impl.DefaultLockManager;
import org.infinispan.util.concurrent.locks.impl.DefaultPendingLockManager;
import org.infinispan.util.concurrent.locks.impl.NoOpPendingLockManager;

/**
 * Factory class that creates instances of {@link LockManager}.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
@DefaultFactoryFor(classes = {LockManager.class, PendingLockManager.class} )
public class LockManagerFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {
   @SuppressWarnings("unchecked")
   @Override
   public <T> T construct(Class<T> componentType) {
      if (PendingLockManager.class.equals(componentType)) {
         final boolean clustered = configuration.clustering().cacheMode().isClustered();
         return clustered ? (T) new DefaultPendingLockManager() : (T) NoOpPendingLockManager.getInstance();
      }
      if (configuration.deadlockDetection().enabled()) {
         return (T) new DeadlockDetectingLockManager();
      } else {
         return (T) new DefaultLockManager();
      }
   }
}
