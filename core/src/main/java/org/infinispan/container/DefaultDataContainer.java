/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.container;

import net.jcip.annotations.ThreadSafe;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionThreadPolicy;
import org.infinispan.metadata.Metadata;
import org.infinispan.util.Equivalence;
import org.infinispan.util.Util;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * DefaultDataContainer is both eviction and non-eviction based data container.
 *
 *
 * @author Manik Surtani
 * @author Galder Zamarreño
 * @author Vladimir Blagojevic
 * @author <a href="http://gleamynode.net/">Trustin Lee</a>
 * @author Pedro Ruivo
 * @since 4.0
 */
@ThreadSafe
public class DefaultDataContainer extends AbstractDataContainer<InternalCacheEntry> {

   public DefaultDataContainer(int concurrencyLevel) {
      super(concurrencyLevel);
   }

   public DefaultDataContainer(int concurrencyLevel, Equivalence keyEq, Equivalence valueEq) {
      super(concurrencyLevel, keyEq, valueEq);
   }

   protected DefaultDataContainer(int concurrencyLevel, int maxEntries, EvictionStrategy strategy, EvictionThreadPolicy policy,
         Equivalence keyEq, Equivalence valueEq) {
      super(concurrencyLevel, maxEntries, strategy, policy, keyEq, valueEq);
   }

   public static DataContainer boundedDataContainer(int concurrencyLevel, int maxEntries,
            EvictionStrategy strategy, EvictionThreadPolicy policy,
            Equivalence keyEquivalence, Equivalence valueEquivalence) {
      return new DefaultDataContainer(concurrencyLevel, maxEntries, strategy,
            policy, keyEquivalence, valueEquivalence);
   }

   public static DataContainer unBoundedDataContainer(int concurrencyLevel,
         Equivalence keyEquivalence, Equivalence valueEquivalence) {
      return new DefaultDataContainer(concurrencyLevel, keyEquivalence, valueEquivalence);
   }

   public static DataContainer unBoundedDataContainer(int concurrencyLevel) {
      return new DefaultDataContainer(concurrencyLevel);
   }

   @Override
   public InternalCacheEntry peek(Object key, Metadata metadata) {
      return entries.get(key);
   }

   @Override
   public InternalCacheEntry get(Object k, Metadata metadata) {
      InternalCacheEntry e = peek(k, metadata);
      if (e != null && e.canExpire()) {
         long currentTimeMillis = timeService.wallClockTime();
         if (e.isExpired(currentTimeMillis)) {
            entries.remove(k);
            e = null;
         } else {
            e.touch(currentTimeMillis);
         }
      }
      return e;
   }

   @Override
   public void put(Object k, Object v, Metadata metadata) {
      InternalCacheEntry e = entries.get(k);
      if (e != null) {
         e.setValue(v);
         InternalCacheEntry original = e;
         e = entryFactory.update(e, metadata);
         // we have the same instance. So we need to reincarnate.
         if (original == e) {
            e.reincarnate(timeService.wallClockTime());
         }
      } else {
         // this is a brand-new entry
         e = entryFactory.create(k, v, metadata);
      }
      entries.put(k, e);
   }

   @Override
   public boolean containsKey(Object k, Metadata metadata) {
      InternalCacheEntry ice = peek(k, metadata);
      if (ice != null && ice.canExpire() && ice.isExpired(timeService.wallClockTime())) {
         entries.remove(k);
         ice = null;
      }
      return ice != null;
   }

   @Override
   public InternalCacheEntry remove(Object k, Metadata metadata) {
      InternalCacheEntry e = entries.remove(k);
      return e == null || (e.canExpire() && e.isExpired(timeService.wallClockTime())) ? null : e;
   }

   @Override
   public int size(Metadata metadata) {
      return entries.size();
   }

   @Override
   public void clear() {
      entries.clear();
   }

   @Override
   public void clear(Metadata metadata) {
      clear();
   }

   @Override
   public void purgeExpired() {
      long currentTimeMillis = timeService.wallClockTime();
      for (Iterator<InternalCacheEntry> purgeCandidates = entries.values().iterator(); purgeCandidates.hasNext();) {
         InternalCacheEntry e = purgeCandidates.next();
         if (e.isExpired(currentTimeMillis)) {
            purgeCandidates.remove();
         }
      }
   }

   @Override
   protected Map<Object, InternalCacheEntry> getCacheEntries(Map<Object, InternalCacheEntry> evicted) {
      return evicted;
   }

   @Override
   protected InternalCacheEntry getCacheEntry(InternalCacheEntry evicted) {
      return evicted;
   }

   @Override
   protected InternalCacheEntry getCacheEntry(InternalCacheEntry entry, Metadata metadata) {
      return entry;
   }

   @Override
   protected EntryIterator createEntryIterator(Metadata metadata) {
      return new DefaultEntryIterator(entries.values().iterator());
   }

   @Override
   public final boolean dumpTo(String filePath) {
      BufferedWriter bufferedWriter = Util.getBufferedWriter(filePath);
      if (bufferedWriter == null) {
         return false;
      }
      try {
         for (Map.Entry<Object, InternalCacheEntry> entry : entries.entrySet()) {
            Util.safeWrite(bufferedWriter, entry.getKey());
            Util.safeWrite(bufferedWriter, "=");
            Util.safeWrite(bufferedWriter, entry.getValue().getValue());
            Util.safeWrite(bufferedWriter, "=");
            Util.safeWrite(bufferedWriter, entry.getValue().getMetadata().version());
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
   public void purgeOldValues(EntryVersion minimumVersion) {
      //no-op
   }

   public static class DefaultEntryIterator extends EntryIterator {

      private final Iterator<InternalCacheEntry> it;

      DefaultEntryIterator(Iterator<InternalCacheEntry> it){this.it=it;}

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
}
