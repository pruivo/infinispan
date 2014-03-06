package org.infinispan.container;

import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.util.concurrent.ParallelIterableMap;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.marshall.core.MarshalledEntryImpl;
import org.infinispan.metadata.InternalMetadata;
import org.infinispan.metadata.InternalMetadataImpl;
import org.infinispan.metadata.Metadata;
import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.util.CoreImmutables;
import org.infinispan.util.TimeService;

import java.util.Iterator;

import static org.infinispan.factories.KnownComponentNames.CACHE_MARSHALLER;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
public abstract class AbstractDataContainer implements DataContainer {

   StreamingMarshaller marshaller;
   private TimeService timeService;
   private InternalEntryFactory entryFactory;

   @Inject
   public void injectDependencies(TimeService timeService, InternalEntryFactory entryFactory,
                                  @ComponentName(CACHE_MARSHALLER) StreamingMarshaller marshaller) {
      this.timeService = timeService;
      this.entryFactory = entryFactory;
      this.marshaller = marshaller;
   }

   @Override
   public final InternalCacheEntry get(Object key, AccessMode mode) {
      assertNotNull("mode", mode);
      return touchEntryOrRemoveIfExpired(innerGet(key, mode));
   }

   @Override
   public final InternalCacheEntry peek(Object key, AccessMode mode) {
      assertNotNull("mode", mode);
      return innerPeek(key, mode);
   }

   @Override
   public final void put(Object key, Object value, Metadata metadata) {
      InternalCacheEntry entry = peek(key, AccessMode.SKIP_PERSISTENCE);
      if (entry == null) {
         entry = entryFactory.create(key, value, metadata);
      } else {
         entry.setValue(value);
         InternalCacheEntry original = entry;
         entry = entryFactory.update(entry, metadata);
         // we have the same instance. So we need to reincarnate, if mortal.
         if (isMortalEntry(entry) && original == entry) {
            entry.reincarnate(timeService.wallClockTime());
         }
      }
      innerPut(entry, AccessMode.ALL);
   }

   @Override
   public final boolean containsKey(Object key, AccessMode mode) {
      assertNotNull("mode", mode);
      InternalCacheEntry entry = touchEntryOrRemoveIfExpired(innerGet(key, mode));
      return entry != null;
   }

   @Override
   public final InternalCacheEntry remove(Object key, AccessMode mode) {
      assertNotNull("mode", mode);
      InternalCacheEntry entry = innerRemove(key, mode);
      return entry == null || isExpired(entry) ? null : entry;
   }

   @Override
   public final int size(AccessMode mode) {
      assertNotNull("mode", mode);
      return innerSize(mode);
   }

   @Override
   public final void purgeExpired() {
      long currentTimeMillis = timeService.wallClockTime();
      for (Iterator<InternalCacheEntry> purgeCandidates = asInMemoryUtil().iterator(); purgeCandidates.hasNext(); ) {
         InternalCacheEntry e = purgeCandidates.next();
         if (e.isExpired(currentTimeMillis)) {
            purgeCandidates.remove();
         }
      }
   }

   @Override
   public final void executeTask(final AdvancedCacheLoader.KeyFilter<Object> filter, final ParallelIterableMap.KeyValueAction<Object, InternalCacheEntry> action)
         throws InterruptedException {
      if (filter == null) {
         throw new NullPointerException("No filter specified");
      }
      if (action == null) {
         throw new NullPointerException("No action specified");
      }


      //noinspection unchecked
      asInMemoryUtil().forEach(512, new ParallelIterableMap.KeyValueAction<Object, InternalCacheEntry>() {
         @Override
         public void apply(Object key, InternalCacheEntry value) {
            if (filter.shouldLoadKey(key)) {
               action.apply(key, value);
            }
         }
      });
      //TODO figure out the way how to do interruption better (during iteration)
      if (Thread.currentThread().isInterrupted()) {
         throw new InterruptedException();
      }
   }

   @Override
   public final void clear() {
      asInMemoryUtil().clear();
   }

   @Override
   public final Iterator<InternalCacheEntry> iterator() {
      return new ImmutableEntryIterator(asInMemoryUtil().iterator());
   }

   /**
    * Same behavior as {@link #peek(Object, DataContainer.AccessMode)}.
    *
    * @return the {@link org.infinispan.container.entries.InternalCacheEntry} without touching it.
    */
   protected abstract InternalCacheEntry innerPeek(Object key, AccessMode mode);

   //load from data container (or persistence).

   /**
    * Same behavior as {@link #innerPeek(Object, DataContainer.AccessMode)} but if the entry
    * is loaded from persistence, then it is stored in the container.
    *
    * @return the {@link org.infinispan.container.entries.InternalCacheEntry} without touching it.
    */
   protected abstract InternalCacheEntry innerGet(Object key, AccessMode mode);

   /**
    * Removes the key from container and/or cache store. Note that if {@link DataContainer.AccessMode#SKIP_PERSISTENCE}
    * is used, the entry should be passivated, i.e., the entry is removed from container and should be stored in cache
    * store (if it wasn't already there).
    *
    * @return the removed {@link org.infinispan.container.entries.InternalCacheEntry}.
    */
   protected abstract InternalCacheEntry innerRemove(Object key, AccessMode mode);

   /**
    * Same behavior as described in {@link #put(Object, Object, org.infinispan.metadata.Metadata)}
    */
   protected abstract void innerPut(InternalCacheEntry entry, AccessMode mode);

   /**
    * Same behavior as described in {@link #size(DataContainer.AccessMode)}
    *
    * @return the number of entries stored.
    */
   protected abstract int innerSize(AccessMode mode);

   /**
    * @return an exposed interface with the common logic to deal with in-memory storage.
    */
   protected abstract MemoryContainerUtil asInMemoryUtil();

   /**
    * Converts the {@link org.infinispan.marshall.core.MarshalledEntry} into {@link
    * org.infinispan.container.entries.InternalCacheEntry}
    *
    * @return the converted {@code InternalCacheEntry} or {@code null} if the parameter {@code marshalledEntry} is null.
    */
   protected final InternalCacheEntry convert(MarshalledEntry marshalledEntry) {
      if (marshalledEntry == null) {
         return null;
      }
      return entryFactory.create(marshalledEntry.getKey(), marshalledEntry.getValue(), marshalledEntry.getMetadata());
   }

   /**
    * Converts the {@link org.infinispan.container.entries.InternalCacheEntry} into {@link
    * org.infinispan.marshall.core.MarshalledEntry}
    *
    * @return the converted {@code MarshalledEntry} or {@code null} if the parameter {@code entry} is null.
    */
   protected final MarshalledEntry convert(InternalCacheEntry entry) {
      if (entry == null) {
         return null;
      }
      return new MarshalledEntryImpl<Object, Object>(entry.getKey(), entry.getValue(),
                                                     internalMetadata(entry.toInternalCacheValue()), marshaller);
   }

   /**
    * @return the exception to be thrown if the {@link DataContainer.AccessMode} is not
    * valid.
    */
   protected static IllegalArgumentException illegalAccessMode(AccessMode mode) {
      return new IllegalArgumentException("Invalid access mode: " + mode);
   }

   private boolean isExpired(InternalCacheEntry entry) {
      return entry.canExpire() && entry.isExpired(timeService.wallClockTime());
   }

   private InternalCacheEntry touch(InternalCacheEntry entry) {
      entry.touch(timeService.wallClockTime());
      return entry;
   }

   private boolean isMortalEntry(InternalCacheEntry entry) {
      return entry.getLifespan() > 0;
   }

   private InternalCacheEntry touchEntryOrRemoveIfExpired(InternalCacheEntry entry) {
      if (isExpired(entry)) {
         innerRemove(entry.getKey(), AccessMode.ALL);
         return null;
      }
      return touch(entry);
   }

   private void assertNotNull(String fieldName, Object value) {
      if (value == null) {
         throw new NullPointerException(fieldName + " must not be null.");
      }
   }

   private static InternalMetadata internalMetadata(InternalCacheValue icv) {
      return icv.getMetadata() == null ? null : new InternalMetadataImpl(icv.getMetadata(), icv.getCreated(), icv.getLastUsed());
   }

   protected static interface MemoryContainerUtil extends ParallelIterableMap<Object, InternalCacheEntry>,
                                                          Iterable<InternalCacheEntry> {
      void clear();
   }

   private static class ImmutableEntryIterator implements Iterator<InternalCacheEntry> {

      private final Iterator<InternalCacheEntry> it;

      ImmutableEntryIterator(Iterator<InternalCacheEntry> it) {
         this.it = it;
      }

      @Override
      public InternalCacheEntry next() {
         return CoreImmutables.immutableInternalCacheEntry(it.next());
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
}
