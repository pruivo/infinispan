package org.infinispan.container;

import com.sun.istack.internal.NotNull;
import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.eviction.ActivationManager;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionThreadPolicy;
import org.infinispan.eviction.PassivationManager;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.util.concurrent.BoundedConcurrentHashMap;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static org.infinispan.util.concurrent.BoundedConcurrentHashMap.EvictionListener;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
public class DurableBoundedDataContainer extends AbstractDataContainer {

   private final BoundedConcurrentHashMap<Object, InternalCacheEntry> entries;
   private final MemoryContainerUtil memoryContainerUtil = new MemoryContainerUtil() {
      @Override
      public void clear() {
         entries.clear();
      }

      @Override
      public Iterator<InternalCacheEntry> iterator() {
         return entries.values().iterator();
      }

      @Override
      public void forEach(long parallelismThreshold, KeyValueAction<? super Object, ? super InternalCacheEntry> action) throws InterruptedException {
         entries.forEach(parallelismThreshold, action);
      }
   };
   private PersistenceManager persistenceManager;
   private ActivationManager activationManager;
   private PassivationManager passivationManager;

   public DurableBoundedDataContainer(int concurrencyLevel, int maxEntries, EvictionStrategy strategy,
                                      EvictionThreadPolicy policy, Equivalence<Object> keyEquivalence,
                                      Equivalence<InternalCacheEntry> valueEquivalence) {
      // translate eviction policy and strategy
      if (policy != EvictionThreadPolicy.PIGGYBACK && policy != EvictionThreadPolicy.DEFAULT) {
         throw new IllegalArgumentException("No such eviction thread policy " + strategy);
      }

      BoundedConcurrentHashMap.Eviction eviction;
      switch (strategy) {
         case FIFO:
         case UNORDERED:
         case LRU:
            eviction = BoundedConcurrentHashMap.Eviction.LRU;
            break;
         case LIRS:
            eviction = BoundedConcurrentHashMap.Eviction.LIRS;
            break;
         default:
            throw new IllegalArgumentException("No such eviction strategy " + strategy);
      }

      entries = new BoundedConcurrentHashMap<Object, InternalCacheEntry>(
            maxEntries, concurrencyLevel, eviction, new DefaultEvictionListener(),
            keyEquivalence, valueEquivalence);
   }

   @Override
   public Set<Object> keySet(AccessMode mode) {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public Collection<Object> values(AccessMode mode) {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public Set<InternalCacheEntry> entrySet(AccessMode mode) {
      return null;  // TODO: Customise this generated block
   }

   @Override
   protected InternalCacheEntry innerGet(@NotNull Object key, @NotNull AccessMode mode) {
      return null;  // TODO: Customise this generated block
   }

   @Override
   protected InternalCacheEntry innerRemove(@NotNull Object key, @NotNull AccessMode mode) {
      return null;  // TODO: Customise this generated block
   }

   @Override
   protected void innerPut(@NotNull InternalCacheEntry entry, @NotNull AccessMode mode) {
      // TODO: Customise this generated block
   }

   @Override
   protected int innerSize(@NotNull AccessMode mode) {
      return 0;  // TODO: Customise this generated block
   }

   @Override
   protected MemoryContainerUtil asInMemoryUtil() {
      return null;  // TODO: Customise this generated block
   }

   private final class DefaultEvictionListener implements EvictionListener<Object, InternalCacheEntry> {

      @Override
      public void onEntryEviction(Map<Object, InternalCacheEntry> evicted) {
         evictionManager.onEntryEviction(evicted);
      }

      @Override
      public void onEntryChosenForEviction(InternalCacheEntry entry) {
         passivationManager.passivate(entry);
      }

      @Override
      public void onEntryActivated(Object key) {
         activationManager.activate(key);
      }

      @Override
      public void onEntryRemoved(Object key) {
         persistenceManager.deleteFromAllStores(key, false);
      }
   }
}
