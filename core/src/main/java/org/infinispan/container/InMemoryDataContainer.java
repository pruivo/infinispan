package org.infinispan.container;

import com.sun.istack.internal.NotNull;
import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.commons.util.concurrent.ParallelIterableMap;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionThreadPolicy;
import org.infinispan.util.CoreImmutables;
import org.infinispan.util.concurrent.BoundedConcurrentHashMap;

import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import static org.infinispan.util.concurrent.BoundedConcurrentHashMap.EvictionListener;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
public class InMemoryDataContainer extends AbstractDataContainer {

   private static EvictionListener<Object, InternalCacheEntry> NO_OP_LISTENER =
         new EvictionListener<Object, InternalCacheEntry>() {
            @Override
            public void onEntryEviction(Map<Object, InternalCacheEntry> evicted) {/*no-op*/}

            @Override
            public void onEntryChosenForEviction(InternalCacheEntry internalCacheEntry) {/*no-op*/}

            @Override
            public void onEntryActivated(Object key) {/*no-op*/}

            @Override
            public void onEntryRemoved(Object key) {/*no-op*/}
         };
   private final ConcurrentMap<Object, InternalCacheEntry> entries;
   private final MemoryContainerUtil memoryContainerUtil = new MemoryContainerUtil() {
      @Override
      public void clear() {
         entries.clear();
      }

      @Override
      public Iterator<InternalCacheEntry> iterator() {
         return entries.values().iterator();
      }

      @SuppressWarnings("unchecked")
      @Override
      public void forEach(long parallelismThreshold, KeyValueAction<? super Object, ? super InternalCacheEntry> action) throws InterruptedException {
         ((ParallelIterableMap<Object, InternalCacheEntry>) entries).forEach(parallelismThreshold, action);
      }
   };

   private InMemoryDataContainer(ConcurrentMap<Object, InternalCacheEntry> map) {
      this.entries = map;
   }

   public static InMemoryDataContainer unboundedDataContainer(int concurrencyLevel) {
      return newInstance(CollectionFactory.<Object, InternalCacheEntry>makeConcurrentMap(128, concurrencyLevel));
   }

   public static InMemoryDataContainer unboundedDataContainer(int concurrencyLevel, Equivalence<Object> keyEquivalence,
                                                              Equivalence<InternalCacheEntry> valueEquivalence) {
      return newInstance(CollectionFactory.makeConcurrentMap(128, concurrencyLevel, keyEquivalence, valueEquivalence));
   }

   public static InMemoryDataContainer boundedDataContainer(int concurrencyLevel, int maxEntries,
                                                            EvictionStrategy strategy, EvictionThreadPolicy policy,
                                                            Equivalence<Object> keyEquivalence,
                                                            Equivalence<InternalCacheEntry> valueEquivalence) {
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

      ConcurrentMap<Object, InternalCacheEntry> map = new BoundedConcurrentHashMap<Object, InternalCacheEntry>(
            maxEntries, concurrencyLevel, eviction, NO_OP_LISTENER,
            keyEquivalence, valueEquivalence);
      return newInstance(map);
   }

   @Override
   public Set<Object> keySet(AccessMode mode) {
      if (mode == AccessMode.SKIP_CONTAINER) {
         return Collections.emptySet();
      }
      return Collections.unmodifiableSet(entries.keySet());
   }

   @Override
   public Collection<Object> values(AccessMode mode) {
      if (mode == AccessMode.SKIP_CONTAINER) {
         return Collections.emptyList();
      }
      return new Values();
   }

   @Override
   public Set<InternalCacheEntry> entrySet(AccessMode mode) {
      return new EntrySet();
   }

   @Override
   protected InternalCacheEntry innerGet(@NotNull Object key, @NotNull AccessMode mode) {
      return entries.get(key);
   }

   @Override
   protected InternalCacheEntry innerRemove(@NotNull Object key, @NotNull AccessMode mode) {
      return entries.remove(key);
   }

   @Override
   protected void innerPut(@NotNull InternalCacheEntry entry, @NotNull AccessMode mode) {
      entries.put(entry.getKey(), entry);
   }

   @Override
   protected int innerSize(@NotNull AccessMode mode) {
      return entries.size();
   }

   @Override
   protected MemoryContainerUtil asInMemoryUtil() {
      return memoryContainerUtil;
   }

   private static InMemoryDataContainer newInstance(ConcurrentMap<Object, InternalCacheEntry> map) {
      if (map instanceof ParallelIterableMap) {
         return new InMemoryDataContainer(map);
      }
      throw new IllegalStateException("Unable to build in-memory data container");
   }

   private static class ImmutableEntryIterator extends EntryIterator {
      ImmutableEntryIterator(Iterator<InternalCacheEntry> it) {
         super(it);
      }

      @Override
      public InternalCacheEntry next() {
         return CoreImmutables.immutableInternalCacheEntry(super.next());
      }
   }

   public static class EntryIterator implements Iterator<InternalCacheEntry> {

      private final Iterator<InternalCacheEntry> it;

      EntryIterator(Iterator<InternalCacheEntry> it) {
         this.it = it;
      }

      @Override
      public InternalCacheEntry next() {
         return it.next();
      }

      @Override
      public boolean hasNext() {
         return it.hasNext();
      }

      @Override
      public void remove() {
         throw new UnsupportedOperationException();
      }
   }

   private static class ValueIterator implements Iterator<Object> {
      Iterator<InternalCacheEntry> currentIterator;

      private ValueIterator(Iterator<InternalCacheEntry> it) {
         currentIterator = it;
      }

      @Override
      public boolean hasNext() {
         return currentIterator.hasNext();
      }

      @Override
      public void remove() {
         throw new UnsupportedOperationException();
      }

      @Override
      public Object next() {
         return currentIterator.next().getValue();
      }
   }

   /**
    * Minimal implementation needed for unmodifiable Set
    */
   private class EntrySet extends AbstractSet<InternalCacheEntry> {

      @Override
      public boolean contains(Object o) {
         if (!(o instanceof Map.Entry)) {
            return false;
         }

         @SuppressWarnings("rawtypes")
         Map.Entry e = (Map.Entry) o;
         InternalCacheEntry ice = entries.get(e.getKey());
         return ice != null && ice.getValue().equals(e.getValue());
      }

      @Override
      public Iterator<InternalCacheEntry> iterator() {
         return new ImmutableEntryIterator(entries.values().iterator());
      }

      @Override
      public int size() {
         return entries.size();
      }

      @Override
      public String toString() {
         return entries.toString();
      }
   }

   /**
    * Minimal implementation needed for unmodifiable Collection
    */
   private class Values extends AbstractCollection<Object> {
      @Override
      public Iterator<Object> iterator() {
         return new ValueIterator(entries.values().iterator());
      }

      @Override
      public int size() {
         return entries.size();
      }
   }
}
