/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE. See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA 02110-1301, USA.
 */

package org.infinispan.container;

import org.infinispan.CacheException;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.eviction.ActivationManager;
import org.infinispan.eviction.EvictionManager;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionThreadPolicy;
import org.infinispan.eviction.PassivationManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.CacheStore;
import org.infinispan.metadata.Metadata;
import org.infinispan.util.CollectionFactory;
import org.infinispan.util.Equivalence;
import org.infinispan.util.Immutables;
import org.infinispan.util.TimeService;
import org.infinispan.util.concurrent.BoundedConcurrentHashMap;

import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
public abstract class AbstractDataContainer<T> implements DataContainer {

   protected final ConcurrentMap<Object, T> entries;
   final protected DefaultEvictionListener evictionListener;
   protected InternalEntryFactory entryFactory;
   protected TimeService timeService;
   private EvictionManager evictionManager;
   private PassivationManager passivator;
   private ActivationManager activator;
   private CacheLoaderManager clm;

   protected AbstractDataContainer(int concurrencyLevel) {
      // If no comparing implementations passed, could fallback on JDK CHM
      entries = CollectionFactory.makeConcurrentMap(128, concurrencyLevel);
      evictionListener = null;
   }

   protected AbstractDataContainer(int concurrencyLevel, Equivalence keyEq, Equivalence valueEq) {
      // If at least one comparing implementation give, use ComparingCHMv8
      entries = CollectionFactory.<Object, T>makeConcurrentMap(128, concurrencyLevel, keyEq, valueEq);
      evictionListener = null;
   }

   protected AbstractDataContainer(int concurrencyLevel, int maxEntries, EvictionStrategy strategy,
                                   EvictionThreadPolicy policy, Equivalence keyEquivalence,
                                   Equivalence valueEquivalence) {
      // translate eviction policy and strategy
      switch (policy) {
         case PIGGYBACK:
         case DEFAULT:
            evictionListener = new DefaultEvictionListener();
            break;
         default:
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

      entries = new BoundedConcurrentHashMap<Object, T>(
            maxEntries, concurrencyLevel, eviction, evictionListener,
            keyEquivalence, valueEquivalence);
   }

   @Inject
   public void initialize(EvictionManager evictionManager, PassivationManager passivator,
                          InternalEntryFactory entryFactory, ActivationManager activator, CacheLoaderManager clm,
                          TimeService timeService) {
      this.evictionManager = evictionManager;
      this.passivator = passivator;
      this.entryFactory = entryFactory;
      this.activator = activator;
      this.clm = clm;
      this.timeService = timeService;

   }

   @Override
   public Set<Object> keySet(Metadata metadata) {
      return Collections.unmodifiableSet(entries.keySet());
   }

   @Override
   public Collection<Object> values(Metadata metadata) {
      return new Values(metadata);
   }

   @Override
   public Set<InternalCacheEntry> entrySet(Metadata metadata) {
      return new EntrySet(metadata);
   }

   @Override
   public Iterator<InternalCacheEntry> iterator() {
      return createEntryIterator(null);
   }

   protected abstract Map<Object, InternalCacheEntry> getCacheEntries(Map<Object, T> evicted);

   protected abstract InternalCacheEntry getCacheEntry(T evicted);

   protected abstract InternalCacheEntry getCacheEntry(T entry, Metadata metadata);

   protected abstract EntryIterator createEntryIterator(Metadata metadata);

   protected abstract static class EntryIterator implements Iterator<InternalCacheEntry> {
   }

   private final class DefaultEvictionListener implements BoundedConcurrentHashMap.EvictionListener<Object, T> {

      @Override
      public void onEntryEviction(Map<Object, T> evicted) {
         evictionManager.onEntryEviction(getCacheEntries(evicted));
      }

      @Override
      public void onEntryChosenForEviction(T entry) {
         passivator.passivate(getCacheEntry(entry));
      }

      @Override
      public void onEntryActivated(Object key) {
         activator.activate(key);
      }

      @Override
      public void onEntryRemoved(Object key) {
         try {
            CacheStore cacheStore = clm.getCacheStore();
            if (cacheStore != null)
               cacheStore.remove(key);
         } catch (CacheLoaderException e) {
            throw new CacheException(e);
         }
      }
   }

   private class ImmutableEntryIterator implements Iterator<InternalCacheEntry> {

      private final EntryIterator entryIterator;

      ImmutableEntryIterator(EntryIterator entryIterator) {
         this.entryIterator = entryIterator;
      }

      @Override
      public boolean hasNext() {
         return entryIterator.hasNext();
      }

      @Override
      public InternalCacheEntry next() {
         return Immutables.immutableInternalCacheEntry(entryIterator.next());
      }

      @Override
      public void remove() {
         entryIterator.remove();
      }
   }

   /**
    * Minimal implementation needed for unmodifiable Set
    */
   public class EntrySet extends AbstractSet<InternalCacheEntry> {

      private final Metadata metadata;

      public EntrySet(Metadata metadata) {
         this.metadata = metadata;
      }

      @Override
      public boolean contains(Object o) {
         if (!(o instanceof Map.Entry)) {
            return false;
         }

         Map.Entry e = (Map.Entry) o;
         InternalCacheEntry ice = getCacheEntry(entries.get(e.getKey()), metadata);
         return ice != null && ice.getValue().equals(e.getValue());
      }

      @Override
      public Iterator<InternalCacheEntry> iterator() {
         return new ImmutableEntryIterator(createEntryIterator(metadata));
      }

      @Override
      public int size() {
         return AbstractDataContainer.this.size(metadata);
      }

      @Override
      public String toString() {
         return AbstractDataContainer.this.toString();
      }
   }

   /**
    * Minimal implementation needed for unmodifiable Collection
    */
   private class Values extends AbstractCollection<Object> {

      private final Metadata metadata;

      private Values(Metadata metadata) {
         this.metadata = metadata;
      }

      @Override
      public Iterator<Object> iterator() {
         return new ValueIterator(createEntryIterator(metadata));
      }

      @Override
      public int size() {
         return AbstractDataContainer.this.size(metadata);
      }
   }

   private class ValueIterator implements Iterator<Object> {
      private final EntryIterator currentIterator;

      private ValueIterator(EntryIterator it) {
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
}
