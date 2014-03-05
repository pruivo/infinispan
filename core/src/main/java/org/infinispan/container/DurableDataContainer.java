package org.infinispan.container;

import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.commons.util.concurrent.jdk8backported.ConcurrentParallelHashMapV8;
import org.infinispan.commons.util.concurrent.jdk8backported.EquivalentConcurrentHashMapV8;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.eviction.ActivationManager;
import org.infinispan.eviction.PassivationManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.persistence.CollectionKeyFilter;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.util.concurrent.ConcurrentHashSet;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import static org.infinispan.commons.util.concurrent.jdk8backported.EquivalentConcurrentHashMapV8.BiFun;
import static org.infinispan.persistence.spi.AdvancedCacheLoader.CacheLoaderTask;
import static org.infinispan.persistence.spi.AdvancedCacheLoader.KeyFilter.LOAD_ALL_FILTER;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
public class DurableDataContainer extends AbstractDataContainer {

   private final ConcurrentParallelHashMapV8<Object, InternalCacheEntry> entries;
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

   private DurableDataContainer(ConcurrentParallelHashMapV8<Object, InternalCacheEntry> entries) {
      this.entries = entries;
   }

   public static DurableDataContainer newInstance(int concurrencyLevel) {
      return newInstance(CollectionFactory.<Object, InternalCacheEntry>makeConcurrentMap(128, concurrencyLevel));
   }

   public static DurableDataContainer newInstance(int concurrencyLevel, Equivalence<Object> keyEquivalence,
                                                  Equivalence<InternalCacheEntry> valueEquivalence) {
      return newInstance(CollectionFactory.makeConcurrentMap(128, concurrencyLevel, keyEquivalence, valueEquivalence));
   }

   @Inject
   public void injectPersistence(PersistenceManager persistenceManager, ActivationManager activationManager,
                                 PassivationManager passivationManager) {
      this.persistenceManager = persistenceManager;
      this.activationManager = activationManager;
      this.passivationManager = passivationManager;
   }

   @Override
   public Set<Object> keySet(AccessMode mode) {
      switch (mode) {
         case SKIP_CONTAINER:
            final ConcurrentHashSet<Object> persistenceKeys = new ConcurrentHashSet<Object>();
            persistenceManager.processOnAllStores(LOAD_ALL_FILTER, new CacheLoaderTask() {
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
            entries.forEachKey(512, new EquivalentConcurrentHashMapV8.Action<Object>() {
               @Override
               public void apply(Object o) {
                  union.add(o);
               }
            });
            persistenceManager.processOnAllStores(new CollectionKeyFilter(union), new CacheLoaderTask() {
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
            persistenceManager.processOnAllStores(LOAD_ALL_FILTER, new CacheLoaderTask() {
               @Override
               public void processEntry(MarshalledEntry marshalledEntry, AdvancedCacheLoader.TaskContext taskContext) throws InterruptedException {
                  result.add(marshalledEntry.getValue());
               }
            }, true, false);
            return Collections.unmodifiableCollection(result);
         case SKIP_PERSISTENCE:
            entries.forEachValue(512, new EquivalentConcurrentHashMapV8.Action<InternalCacheEntry>() {
               @Override
               public void apply(InternalCacheEntry entry) {
                  result.add(entry.getValue());
               }
            });
            return Collections.unmodifiableCollection(result);
         case ALL:
            final ConcurrentHashSet<Object> skipKeys = new ConcurrentHashSet<Object>();
            entries.forEachValue(512, new EquivalentConcurrentHashMapV8.Action<InternalCacheEntry>() {
               @Override
               public void apply(InternalCacheEntry entry) {
                  result.add(entry.getValue());
                  skipKeys.add(entry.getKey());
               }
            });
            persistenceManager.processOnAllStores(new CollectionKeyFilter(skipKeys), new CacheLoaderTask() {
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
            persistenceManager.processOnAllStores(LOAD_ALL_FILTER, new CacheLoaderTask() {
               @Override
               public void processEntry(MarshalledEntry marshalledEntry, AdvancedCacheLoader.TaskContext taskContext)
                     throws InterruptedException {
                  result.add(convert(marshalledEntry));
               }
            }, true, true);
            return Collections.unmodifiableSet(result);
         case SKIP_PERSISTENCE:
            entries.forEachValue(512, new EquivalentConcurrentHashMapV8.Action<InternalCacheEntry>() {
               @Override
               public void apply(InternalCacheEntry entry) {
                  result.add(entry);
               }
            });
            return Collections.unmodifiableSet(result);
         case ALL:
            final ConcurrentHashSet<Object> skipKeys = new ConcurrentHashSet<Object>();
            entries.forEachValue(512, new EquivalentConcurrentHashMapV8.Action<InternalCacheEntry>() {
               @Override
               public void apply(InternalCacheEntry entry) {
                  skipKeys.add(entry.getKey());
                  result.add(entry);
               }
            });
            persistenceManager.processOnAllStores(new CollectionKeyFilter(skipKeys), new CacheLoaderTask() {
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
            return entries.get(key);
         case ALL:
            InternalCacheEntry entry = entries.get(key);
            final AtomicReference<InternalCacheEntry> loadedEntry = new AtomicReference<InternalCacheEntry>(null);
            if (entry == null) {
               //loaded in compute method to avoid concurrent loads. Remember that peek() does not change the
               //entries maps!
               entry = entries.computeIfAbsent(key, new EquivalentConcurrentHashMapV8.Fun<Object, InternalCacheEntry>() {
                  @Override
                  public InternalCacheEntry apply(Object o) {
                     loadedEntry.set(convert(persistenceManager.loadFromAllStores(key, null)));
                     return null;
                  }
               });
            }
            return entry != null ? entry : loadedEntry.get();
         default:
            throw illegalAccessMode(mode);
      }
   }

   @Override
   protected InternalCacheEntry innerGet(final Object key, AccessMode mode) {
      //TODO how to pass the context?
      switch (mode) {
         case SKIP_CONTAINER:
            return convert(persistenceManager.loadFromAllStores(key, null));
         case SKIP_PERSISTENCE:
            return entries.get(key);
         case ALL:
            InternalCacheEntry entry = entries.get(key);
            if (entry == null) {
               entry = entries.computeIfAbsent(key, new EquivalentConcurrentHashMapV8.Fun<Object, InternalCacheEntry>() {
                  @Override
                  public InternalCacheEntry apply(Object o) {
                     MarshalledEntry marshalledEntry = persistenceManager.loadFromAllStores(key, null);
                     if (marshalledEntry == null) {
                        return null;
                     }
                     activationManager.activate(key);
                     return convert(marshalledEntry);
                  }
               });
            }
            return entry;
         default:
            throw illegalAccessMode(mode);
      }
   }

   @Override
   protected InternalCacheEntry innerRemove(final Object key, AccessMode mode) {
      final AtomicReference<InternalCacheEntry> oldValue = new AtomicReference<InternalCacheEntry>(null);
      switch (mode) {
         case SKIP_PERSISTENCE:
            //i.e. eviction
            entries.computeIfPresent(key, new BiFun<Object, InternalCacheEntry, InternalCacheEntry>() {
               @Override
               public InternalCacheEntry apply(Object o, InternalCacheEntry entry) {
                  if (entry == null) {
                     return null;
                  }
                  oldValue.set(entry);
                  passivationManager.passivate(entry);
                  return null;
               }
            });
            return oldValue.get();
         case ALL:
            entries.computeIfPresent(key, new BiFun<Object, InternalCacheEntry, InternalCacheEntry>() {
               @Override
               public InternalCacheEntry apply(Object o, InternalCacheEntry entry) {
                  if (entry == null) {
                     return null;
                  }
                  oldValue.set(entry);
                  persistenceManager.deleteFromAllStores(key, false);
                  return null;
               }
            });
            return oldValue.get();
         default:
            throw illegalAccessMode(mode);
      }
   }

   @Override
   protected void innerPut(final InternalCacheEntry entry, AccessMode mode) {
      switch (mode) {
         case SKIP_CONTAINER:
            persistenceManager.writeToAllStores(convert(entry), false);
            return;
         case SKIP_PERSISTENCE:
            entries.put(entry.getKey(), entry);
            return;
         case ALL:
            entries.compute(entry.getKey(), new BiFun<Object, InternalCacheEntry, InternalCacheEntry>() {
               @Override
               public InternalCacheEntry apply(Object o, InternalCacheEntry oldEntry) {
                  persistenceManager.writeToAllStores(null, false);
                  return entry;
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
            throw illegalAccessMode(mode);
      }
   }

   @Override
   protected MemoryContainerUtil asInMemoryUtil() {
      return memoryContainerUtil;
   }

   private static DurableDataContainer newInstance(ConcurrentMap<Object, InternalCacheEntry> map) {
      if (map instanceof ConcurrentParallelHashMapV8) {
         return new DurableDataContainer((ConcurrentParallelHashMapV8<Object, InternalCacheEntry>) map);
      }
      throw new IllegalStateException("Unable to build in-memory data container");
   }

}
