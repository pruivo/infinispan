package org.infinispan.factories;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.container.DataContainer;
import org.infinispan.container.DurableBoundedDataContainer;
import org.infinispan.container.DurableDataContainer;
import org.infinispan.container.InMemoryDataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionThreadPolicy;
import org.infinispan.factories.annotations.DefaultFactoryFor;

/**
 * Constructs the data container
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Vladimir Blagojevic
 * @since 4.0
 */
@DefaultFactoryFor(classes = DataContainer.class)
public class DataContainerFactory extends AbstractNamedCacheComponentFactory implements
                                                                             AutoInstantiableFactory {

   @Override
   @SuppressWarnings("unchecked")
   public <T> T construct(Class<T> componentType) {
      if (configuration.dataContainer().dataContainer() != null) {
         return (T) configuration.dataContainer().dataContainer();
      } else {
         final boolean persistence = configuration.persistence().usingStores();
         EvictionStrategy st = configuration.eviction().strategy();
         int level = configuration.locking().concurrencyLevel();
         Equivalence<Object> keyEquivalence = configuration.dataContainer().keyEquivalence();
         Equivalence<InternalCacheEntry> valueEquivalence = configuration.dataContainer().valueEquivalence();

         switch (st) {
            case NONE:
               return (T) unBoundedDataContainer(persistence, level, keyEquivalence, valueEquivalence);
            case UNORDERED:
            case LRU:
            case FIFO:
            case LIRS:
               int maxEntries = configuration.eviction().maxEntries();
               //handle case when < 0 value signifies unbounded container 
               if (maxEntries < 0) {
                  return (T) unBoundedDataContainer(persistence, level, keyEquivalence, valueEquivalence);
               }

               EvictionThreadPolicy policy = configuration.eviction().threadPolicy();

               return (T) boundedDataContainer(persistence, level, maxEntries, st, policy, keyEquivalence,
                                               valueEquivalence);
            default:
               throw new CacheConfigurationException("Unknown eviction strategy "
                                                           + configuration.eviction().strategy());
         }
      }
   }

   private static DataContainer unBoundedDataContainer(boolean persistence, int concurrencyLevel,
                                                       Equivalence<Object> keyEquivalence,
                                                       Equivalence<InternalCacheEntry> valueEquivalence) {
      return persistence ? DurableDataContainer.newInstance(concurrencyLevel, keyEquivalence, valueEquivalence) :
            InMemoryDataContainer.unboundedDataContainer(concurrencyLevel, keyEquivalence, valueEquivalence);
   }

   private static DataContainer boundedDataContainer(boolean persistence, int concurrencyLevel, int maxEntries,
                                                     EvictionStrategy strategy, EvictionThreadPolicy policy,
                                                     Equivalence<Object> keyEquivalence,
                                                     Equivalence<InternalCacheEntry> valueEquivalence) {
      return persistence ? DurableBoundedDataContainer.newInstance(concurrencyLevel, maxEntries, strategy, policy,
                                                                   keyEquivalence, valueEquivalence) :
            InMemoryDataContainer.boundedDataContainer(concurrencyLevel, maxEntries, strategy, policy, keyEquivalence,
                                                       valueEquivalence);
   }
}
