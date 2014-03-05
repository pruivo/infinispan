package org.infinispan.container;

import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.commons.util.concurrent.ParallelIterableMap;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.eviction.ActivationManager;
import org.infinispan.eviction.EvictionManager;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionThreadPolicy;
import org.infinispan.eviction.PassivationManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.persistence.CollectionKeyFilter;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.util.concurrent.BoundedConcurrentHashMap;
import org.infinispan.util.concurrent.ConcurrentHashSet;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.infinispan.persistence.spi.AdvancedCacheLoader.KeyFilter.LOAD_ALL_FILTER;
import static org.infinispan.util.concurrent.BoundedConcurrentHashMap.EvictionListener;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 7.0
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
   private EvictionManager evictionManager;

   private DurableBoundedDataContainer(int concurrencyLevel, int maxEntries, BoundedConcurrentHashMap.Eviction strategy,
                                       Equivalence<Object> keyEquivalence, Equivalence<InternalCacheEntry> valueEquivalence) {
      entries = new BoundedConcurrentHashMap<Object, InternalCacheEntry>(
            maxEntries, concurrencyLevel, strategy, new DefaultEvictionListener(),
            keyEquivalence, valueEquivalence);
   }

   @Inject
   public void injectPersistence(PersistenceManager persistenceManager, ActivationManager activationManager,
                                 PassivationManager passivationManager, EvictionManager evictionManager) {
      this.persistenceManager = persistenceManager;
      this.activationManager = activationManager;
      this.passivationManager = passivationManager;
      this.evictionManager = evictionManager;
   }

   public static DurableBoundedDataContainer newInstance(int concurrencyLevel, int maxEntries, EvictionStrategy strategy,
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
      return new DurableBoundedDataContainer(concurrencyLevel, maxEntries, eviction, keyEquivalence, valueEquivalence);
   }


   @Override
   public Set<Object> keySet(AccessMode mode) {
      switch (mode) {
         case SKIP_CONTAINER:
            final ConcurrentHashSet<Object> persistenceKeys = new ConcurrentHashSet<Object>();
            persistenceManager.processOnAllStores(LOAD_ALL_FILTER, new AdvancedCacheLoader.CacheLoaderTask() {
               @Override
               public void processEntry(MarshalledEntry marshalledEntry, AdvancedCacheLoader.TaskContext taskContext) throws InterruptedException {
                  persistenceKeys.add(marshalledEntry.getKey());
               }
            }, false, false);
            return Collections.unmodifiableSet(persistenceKeys);
         case SKIP_PERSISTENCE:
            return Collections.unmodifiableSet(entries.keySet());
         case ALL:
            final ConcurrentHashSet<Object> union = new ConcurrentHashSet<Object>();
            forEach(new ParallelIterableMap.KeyValueAction<Object, InternalCacheEntry>() {
               @Override
               public void apply(Object o, InternalCacheEntry entry) {
                  union.add(o);
               }
            });
            persistenceManager.processOnAllStores(new CollectionKeyFilter(union), new AdvancedCacheLoader.CacheLoaderTask() {
               @Override
               public void processEntry(MarshalledEntry marshalledEntry, AdvancedCacheLoader.TaskContext taskContext) throws InterruptedException {
                  union.add(marshalledEntry.getKey());
               }
            }, false, false);
            return Collections.unmodifiableSet(union);
         default:
            throw illegalAccessMode(mode);
      }
   }

   @Override
   public Collection<Object> values(AccessMode mode) {
      final ConcurrentLinkedQueue<Object> result = new ConcurrentLinkedQueue<Object>();
      switch (mode) {
         case SKIP_CONTAINER:
            persistenceManager.processOnAllStores(LOAD_ALL_FILTER, new AdvancedCacheLoader.CacheLoaderTask() {
               @Override
               public void processEntry(MarshalledEntry marshalledEntry, AdvancedCacheLoader.TaskContext taskContext) throws InterruptedException {
                  result.add(marshalledEntry.getValue());
               }
            }, true, false);
            return Collections.unmodifiableCollection(result);
         case SKIP_PERSISTENCE:
            forEach(new ParallelIterableMap.KeyValueAction<Object, InternalCacheEntry>() {
               @Override
               public void apply(Object o, InternalCacheEntry entry) {
                  result.add(entry.getValue());
               }
            });
            return Collections.unmodifiableCollection(result);
         case ALL:
            final ConcurrentHashSet<Object> skipKeys = new ConcurrentHashSet<Object>();
            forEach(new ParallelIterableMap.KeyValueAction<Object, InternalCacheEntry>() {
               @Override
               public void apply(Object o, InternalCacheEntry entry) {
                  result.add(entry.getValue());
                  skipKeys.add(entry.getKey());
               }
            });
            persistenceManager.processOnAllStores(new CollectionKeyFilter(skipKeys), new AdvancedCacheLoader.CacheLoaderTask() {
               @Override
               public void processEntry(MarshalledEntry marshalledEntry, AdvancedCacheLoader.TaskContext taskContext) throws InterruptedException {
                  result.add(marshalledEntry.getValue());
               }
            }, true, false);
            return Collections.unmodifiableCollection(result);
         default:
            throw illegalAccessMode(mode);
      }
   }

   @Override
   public Set<InternalCacheEntry> entrySet(AccessMode mode) {
      final ConcurrentHashSet<InternalCacheEntry> result = new ConcurrentHashSet<InternalCacheEntry>();
      switch (mode) {
         case SKIP_CONTAINER:
            persistenceManager.processOnAllStores(LOAD_ALL_FILTER, new AdvancedCacheLoader.CacheLoaderTask() {
               @Override
               public void processEntry(MarshalledEntry marshalledEntry, AdvancedCacheLoader.TaskContext taskContext)
                     throws InterruptedException {
                  result.add(convert(marshalledEntry));
               }
            }, true, true);
            return Collections.unmodifiableSet(result);
         case SKIP_PERSISTENCE:
            forEach(new ParallelIterableMap.KeyValueAction<Object, InternalCacheEntry>() {
               @Override
               public void apply(Object o, InternalCacheEntry entry) {
                  result.add(entry);
               }
            });
            return Collections.unmodifiableSet(result);
         case ALL:
            final ConcurrentHashSet<Object> skipKeys = new ConcurrentHashSet<Object>();
            forEach(new ParallelIterableMap.KeyValueAction<Object, InternalCacheEntry>() {
               @Override
               public void apply(Object o, InternalCacheEntry entry) {
                  skipKeys.add(entry.getKey());
                  result.add(entry);
               }
            });
            persistenceManager.processOnAllStores(new CollectionKeyFilter(skipKeys), new AdvancedCacheLoader.CacheLoaderTask() {
               @Override
               public void processEntry(MarshalledEntry marshalledEntry, AdvancedCacheLoader.TaskContext taskContext)
                     throws InterruptedException {
                  result.add(convert(marshalledEntry));
               }
            }, true, true);
            return Collections.unmodifiableSet(result);
         default:
            throw illegalAccessMode(mode);
      }
   }


   @Override
   protected InternalCacheEntry innerPeek(final Object key, AccessMode mode) {
      switch (mode) {
         case SKIP_CONTAINER:
            return convert(persistenceManager.loadFromAllStores(key, null));
         case SKIP_PERSISTENCE:
            return entries.peek(key);
         case ALL:
            //two phase locking
            InternalCacheEntry entry = entries.peek(key);
            if (entry != null) {
               return entry;
            }
            return performAtomic(key, new AtomicAction<InternalCacheEntry>() {
               @Override
               public InternalCacheEntry execute() {
                  InternalCacheEntry entry = entries.peek(key);
                  if (entry == null) {
                     return convert(persistenceManager.loadFromAllStores(key, null));
                  }
                  return entry;
               }
            });
         default:
            throw illegalAccessMode(mode);
      }
   }

   @Override
   protected InternalCacheEntry innerGet(final Object key, AccessMode mode) {
      switch (mode) {
         case SKIP_CONTAINER:
            return convert(persistenceManager.loadFromAllStores(key, null));
         case SKIP_PERSISTENCE:
            return entries.get(key);
         case ALL:
            //two phase locking
            InternalCacheEntry entry = entries.get(key);
            if (entry != null) {
               return entry;
            }
            return performAtomic(key, new AtomicAction<InternalCacheEntry>() {
               @Override
               public InternalCacheEntry execute() {
                  InternalCacheEntry entry = entries.get(key);
                  if (entry == null) {
                     entry = convert(persistenceManager.loadFromAllStores(key, null));
                     if (entry != null) {
                        //the bounded concurrent hash map invokes the activate(key) internally.
                        entries.put(entry.getKey(), entry);
                     }
                  }
                  return entry;
               }
            });
         default:
            throw illegalAccessMode(mode);
      }
   }

   @Override
   protected InternalCacheEntry innerRemove(Object key, AccessMode mode) {
      switch (mode) {
         case SKIP_PERSISTENCE:
            return entries.evict(key);
         case ALL:
            //implementation: the remove implementation already invokes the deleteFromAllStores();
            return entries.remove(key);
         default:
            throw illegalAccessMode(mode);
      }
   }

   @Override
   protected void innerPut(final InternalCacheEntry entry, AccessMode mode) {
      switch (mode) {
         case SKIP_CONTAINER:
            persistenceManager.writeToAllStores(null, true);
            return;
         case SKIP_PERSISTENCE:
            entries.put(entry.getKey(), entry);
            return;
         case ALL:
            performAtomic(entry.getKey(), new AtomicAction<Void>() {
               @Override
               public Void execute() {
                  entries.put(entry.getKey(), entry);
                  persistenceManager.writeToAllStores(null, true);
                  return null;
               }
            });
            return;
         default:
            throw illegalAccessMode(mode);
      }
   }

   @Override
   protected int innerSize(AccessMode mode) {
      switch (mode) {
         case SKIP_CONTAINER:
            return persistenceManager.size();
         case SKIP_PERSISTENCE:
            return entries.size();
         case ALL:
            return entries.size() + persistenceManager.size();
         default:
            throw new IllegalArgumentException();
      }
   }

   @Override
   protected MemoryContainerUtil asInMemoryUtil() {
      return memoryContainerUtil;
   }

   private void forEach(ParallelIterableMap.KeyValueAction<Object, InternalCacheEntry> action) {
      try {
         entries.forEach(512, action);
      } catch (InterruptedException e) {
         //forEach in concurrent hash map v8 does not throw any interrupt exception. mimic the behavior
         Thread.currentThread().interrupt();
      }
   }

   private <R> R performAtomic(Object key, AtomicAction<R> atomicAction) {
      entries.lock(key);
      try {
         return atomicAction.execute();
      } finally {
         entries.unlock(key);
      }
   }

   private static interface AtomicAction<T> {

      T execute();

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
