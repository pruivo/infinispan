package org.infinispan.container.gmu;

import org.infinispan.container.AbstractDataContainer;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.gmu.InternalGMUNullCacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.gmu.GMUCacheEntryVersion;
import org.infinispan.container.versioning.gmu.GMUReadVersion;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionThreadPolicy;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.metadata.Metadata;
import org.infinispan.transaction.gmu.CommitLog;
import org.infinispan.util.Equivalence;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.infinispan.container.gmu.GMUEntryFactoryImpl.wrap;
import static org.infinispan.transaction.gmu.GMUHelper.convert;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class GMUDataContainer extends AbstractDataContainer<GMUDataContainer.DataContainerVersionChain> {

   private static final Log log = LogFactory.getLog(GMUDataContainer.class);
   private CommitLog commitLog;

   public GMUDataContainer(int concurrencyLevel) {
      super(concurrencyLevel);
   }

   protected GMUDataContainer(int concurrencyLevel, Equivalence keyEq, Equivalence valueEq) {
      super(concurrencyLevel, keyEq, valueEq);
   }

   protected GMUDataContainer(int concurrencyLevel, int maxEntries, EvictionStrategy strategy, EvictionThreadPolicy policy,
                              Equivalence keyEq, Equivalence valueEq) {
      super(concurrencyLevel, maxEntries, strategy, policy, keyEq, valueEq);
   }

   public static DataContainer boundedDataContainer(int concurrencyLevel, int maxEntries,
                                                    EvictionStrategy strategy, EvictionThreadPolicy policy,
                                                    Equivalence keyEquivalence, Equivalence valueEquivalence) {
      return new GMUDataContainer(concurrencyLevel, maxEntries, strategy, policy, keyEquivalence, valueEquivalence);
   }

   public static DataContainer unBoundedDataContainer(int concurrencyLevel,
                                                      Equivalence keyEquivalence, Equivalence valueEquivalence) {
      return new GMUDataContainer(concurrencyLevel, keyEquivalence, valueEquivalence);
   }

   @Inject
   public void setCommitLog(CommitLog commitLog) {
      this.commitLog = commitLog;
   }

   @Override
   public InternalCacheEntry get(Object k, Metadata metadata) {
      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.get(%s,%s)", k, metadata);
      }
      InternalCacheEntry entry = peek(k, metadata);
      long now = System.currentTimeMillis();
      if (entry.canExpire() && entry.isExpired(now)) {
         if (log.isTraceEnabled()) {
            log.tracef("DataContainer.get(%s,%s) => EXPIRED", k, metadata);
         }

         return new InternalGMUNullCacheEntry(entry);
      }
      entry.touch(now);

      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.get(%s,%s) => %s", k, metadata, entry);
      }

      return entry;
   }

   @Override
   public InternalCacheEntry peek(Object k, Metadata metadata) {
      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.peek(%s,%s)", k, metadata);
      }

      DataContainerVersionChain chain = entries.get(k);
      if (chain == null) {
         if (log.isTraceEnabled()) {
            log.tracef("DataContainer.peek(%s,%s) => NOT_FOUND", k, metadata);
         }
         return wrap(k, null, true, metadata.version(), null, null);
      }
      VersionEntry<InternalCacheEntry> entry = chain.get(getReadVersion(metadata));

      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.peek(%s,%s) => %s", k, metadata, entry);
      }
      EntryVersion creationVersion = entry.getEntry() == null ? null : entry.getEntry().getMetadata().version();

      return wrap(k, entry.getEntry(), entry.isMostRecent(), metadata.version(), creationVersion, entry.getNextVersion());
   }

   @Override
   public void put(Object k, Object v, Metadata metadata) {
      EntryVersion version = metadata.version();
      if (version == null) {
         throw new IllegalArgumentException("Key cannot have null versions!");
      }
      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.put(%s,%s,%s)", k, v, metadata);
      }
      GMUCacheEntryVersion cacheEntryVersion = assertGMUCacheEntryVersion(version);
      DataContainerVersionChain chain = entries.get(k);

      if (chain == null) {
         if (log.isTraceEnabled()) {
            log.tracef("DataContainer.put(%s,%s,%s), create new VersionChain", k, v, metadata);
         }
         chain = new DataContainerVersionChain();
         entries.put(k, chain);
      }

      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.put(%s,%s,%s), correct version is %s", k, v, metadata, cacheEntryVersion);
      }

      chain.add(entryFactory.create(k, v, metadata));
      if (log.isTraceEnabled()) {
         StringBuilder stringBuilder = new StringBuilder();
         chain.chainToString(stringBuilder);
         log.tracef("Updated chain is %s", stringBuilder);
      }
   }

   @Override
   public boolean containsKey(Object k, Metadata metadata) {
      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.containsKey(%s,%s)", k, metadata);
      }

      VersionChain chain = entries.get(k);
      boolean contains = chain != null && chain.contains(getReadVersion(metadata));

      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.containsKey(%s,%s) => %s", k, metadata, contains);
      }

      return contains;
   }

   @Override
   public InternalCacheEntry remove(Object k, Metadata metadata) {
      if (metadata == null) {
         throw new IllegalArgumentException("Key cannot have null version!");
      }
      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.remove(%s,%s)", k, metadata);
      }

      DataContainerVersionChain chain = entries.get(k);
      if (chain == null) {
         if (log.isTraceEnabled()) {
            log.tracef("DataContainer.remove(%s,%s) => NOT_FOUND", k, metadata);
         }
         return wrap(k, null, true, null, null, null);
      }
      VersionEntry<InternalCacheEntry> entry = chain.remove(new InternalGMUNullCacheEntry(k, metadata));

      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.remove(%s,%s) => %s", k, metadata, entry);
      }
      return wrap(k, entry.getEntry(), entry.isMostRecent(), null, null, null);
   }

   @Override
   public int size(Metadata metadata) {
      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.size(%s)", metadata);
      }
      int size = 0;
      for (VersionChain chain : entries.values()) {
         if (chain.contains(getReadVersion(metadata))) {
            size++;
         }
      }

      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.size(%s) => %s", metadata, size);
      }
      return size;
   }

   @Override
   public void clear() {
      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.clear()");
      }
      entries.clear();
   }

   @Override
   public void clear(Metadata metadata) {
      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.clear(%s)", metadata);
      }
      for (Object key : entries.keySet()) {
         remove(key, metadata);
      }
   }

   @Override
   public void purgeExpired() {
      long currentTimeMillis = System.currentTimeMillis();
      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.purgeExpired(%s)", currentTimeMillis);
      }
      for (VersionChain chain : entries.values()) {
         chain.purgeExpired(currentTimeMillis);
      }
   }

   @Override
   public final boolean dumpTo(String filePath) {
      BufferedWriter bufferedWriter = Util.getBufferedWriter(filePath);
      if (bufferedWriter == null) {
         return false;
      }
      try {
         for (Map.Entry<Object, DataContainerVersionChain> entry : entries.entrySet()) {
            Util.safeWrite(bufferedWriter, entry.getKey());
            Util.safeWrite(bufferedWriter, "=");
            entry.getValue().dumpChain(bufferedWriter);
            bufferedWriter.newLine();
            bufferedWriter.flush();
         }
         return true;
      } catch (IOException e) {
         return false;
      } finally {
         Util.close(bufferedWriter);
      }
   }

   @Override
   public final void purgeOldValues(EntryVersion minimumVersion) {
      for (DataContainerVersionChain versionChain : entries.values()) {
         versionChain.gc(minimumVersion);
      }
   }

   public final VersionChain<?> getVersionChain(Object key) {
      return entries.get(key);
   }

   public final String stateToString() {
      StringBuilder stringBuilder = new StringBuilder(8132);
      for (Map.Entry<Object, DataContainerVersionChain> entry : entries.entrySet()) {
         stringBuilder.append(entry.getKey())
               .append("=");
         entry.getValue().chainToString(stringBuilder);
         stringBuilder.append("\n");
      }
      return stringBuilder.toString();
   }

   @Override
   protected Map<Object, InternalCacheEntry> getCacheEntries(Map<Object, DataContainerVersionChain> evicted) {
      Map<Object, InternalCacheEntry> evictedMap = new HashMap<Object, InternalCacheEntry>();
      for (Map.Entry<Object, DataContainerVersionChain> entry : evicted.entrySet()) {
         evictedMap.put(entry.getKey(), entry.getValue().get(null).getEntry());
      }
      return evictedMap;
   }

   @Override
   protected InternalCacheEntry getCacheEntry(DataContainerVersionChain evicted) {
      return evicted.get(null).getEntry();
   }

   @Override
   protected InternalCacheEntry getCacheEntry(DataContainerVersionChain entry, Metadata metadata) {
      return entry == null ? null : entry.get(metadata.version()).getEntry();
   }

   @Override
   protected EntryIterator createEntryIterator(Metadata metadata) {
      return new GMUEntryIterator(metadata.version(), entries.values().iterator());
   }

   private GMUCacheEntryVersion assertGMUCacheEntryVersion(EntryVersion entryVersion) {
      return convert(entryVersion, GMUCacheEntryVersion.class);
   }

   private GMUReadVersion getReadVersion(Metadata metadata) {
      GMUReadVersion gmuReadVersion = commitLog.getReadVersion(metadata.version());
      if (log.isDebugEnabled()) {
         log.debugf("getReadVersion(%s) ==> %s", metadata, gmuReadVersion);
      }
      return gmuReadVersion;
   }

   public static class DataContainerVersionChain extends VersionChain<InternalCacheEntry> {

      @Override
      protected VersionBody<InternalCacheEntry> newValue(InternalCacheEntry value) {
         return new DataContainerVersionBody(value);
      }

      @Override
      protected void writeValue(BufferedWriter writer, InternalCacheEntry value) throws IOException {
         writer.write(String.valueOf(value.getValue()));
         writer.write("=");
         writer.write(String.valueOf(value.getMetadata().version()));
      }
   }

   public static class DataContainerVersionBody extends VersionBody<InternalCacheEntry> {

      protected DataContainerVersionBody(InternalCacheEntry value) {
         super(value);
      }

      @Override
      public EntryVersion getVersion() {
         return getValue().getMetadata().version();
      }

      @Override
      public boolean isOlder(VersionBody<InternalCacheEntry> otherBody) {
         return isOlder(getValue().getMetadata().version(), otherBody.getVersion());
      }

      @Override
      public boolean isOlderOrEquals(EntryVersion entryVersion) {
         return isOlderOrEquals(getValue().getMetadata().version(), entryVersion);
      }

      @Override
      public boolean isEqual(VersionBody<InternalCacheEntry> otherBody) {
         return isEqual(getValue().getMetadata().version(), otherBody.getVersion());
      }

      @Override
      public boolean isRemove() {
         return getValue().isRemoved();
      }

      @Override
      public void reincarnate(VersionBody<InternalCacheEntry> other) {
         throw new IllegalStateException("This cannot happen");
      }

      @Override
      public VersionBody<InternalCacheEntry> gc(EntryVersion minVersion) {
         if (isOlderOrEquals(getValue().getMetadata().version(), minVersion)) {
            VersionBody<InternalCacheEntry> previous = getPrevious();
            //GC previous entries, removing all the references to the previous version entry
            setPrevious(null);
            return previous;
         } else {
            return getPrevious();
         }
      }

      @Override
      protected boolean isExpired(long now) {
         InternalCacheEntry entry = getValue();
         return entry != null && entry.canExpire() && entry.isExpired(now);
      }
   }

   private class GMUEntryIterator extends EntryIterator {

      private final EntryVersion version;
      private final Iterator<DataContainerVersionChain> iterator;
      private InternalCacheEntry next;

      private GMUEntryIterator(EntryVersion version, Iterator<DataContainerVersionChain> iterator) {
         this.version = version;
         this.iterator = iterator;
         findNext();
      }

      @Override
      public boolean hasNext() {
         return next != null;
      }

      @Override
      public InternalCacheEntry next() {
         if (next == null) {
            throw new NoSuchElementException();
         }
         InternalCacheEntry toReturn = next;
         findNext();
         return toReturn;
      }

      @Override
      public void remove() {
         throw new UnsupportedOperationException();
      }

      private void findNext() {
         next = null;
         while (iterator.hasNext()) {
            DataContainerVersionChain chain = iterator.next();
            next = chain.get(version).getEntry();
            if (next != null) {
               return;
            }
         }
      }
   }
}
