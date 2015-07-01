package org.infinispan.factories;

import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.concurrent.locks.impl.DefaultLockManager;

/**
 * Factory class that creates instances of {@link LockManager}.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
@DefaultFactoryFor(classes = LockManager.class)
public class LockManagerFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {
   @SuppressWarnings("unchecked")
   @Override
   public <T> T construct(Class<T> componentType) {
      if (configuration.deadlockDetection().enabled()) {
         //TODO
         return (T) new DefaultLockManager();
      } else {
         return (T) new DefaultLockManager();
      }
   }
}
