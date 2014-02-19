package org.infinispan.container;

import com.sun.istack.internal.NotNull;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.metadata.Metadata;
import org.infinispan.util.TimeService;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
public abstract class AbstractDataContainer implements DataContainerV2 {

   private TimeService timeService;
   private InternalEntryFactory entryFactory;

   @Inject
   public void injectTimeServerAndFactory(TimeService timeService, InternalEntryFactory entryFactory) {
      this.timeService = timeService;
      this.entryFactory = entryFactory;
   }

   @Override
   public final InternalCacheEntry get(Object key, AccessMode mode) {
      assertNotNull("mode", mode);
      return touchEntryOrRemoveIfExpired(innerGet(key, mode));
   }

   @Override
   public final InternalCacheEntry peek(Object key, AccessMode mode) {
      assertNotNull("mode", mode);
      return innerGet(key, mode);
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

   //load from data container (or persistence).
   protected abstract InternalCacheEntry innerGet(@NotNull Object key, @NotNull AccessMode mode);

   //remove (options define if remove from persistence)
   protected abstract InternalCacheEntry innerRemove(@NotNull Object key, @NotNull AccessMode mode);

   //put in data container and in persistence
   protected abstract void innerPut(@NotNull InternalCacheEntry entry, @NotNull AccessMode mode);

   protected abstract int innerSize(@NotNull AccessMode mode);

   protected final TimeService getTimeService() {
      return timeService;
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
}
