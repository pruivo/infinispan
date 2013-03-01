package org.infinispan.stats.topK;

import com.clearspring.analytics.stream.Counter;
import com.clearspring.analytics.stream.StreamSummary;
import org.infinispan.Cache;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This contains all the stream lib top keys. Stream lib is a space efficient technique to obtains the top-most
 * counters.
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class StreamLibContainer {

   public static final int MAX_CAPACITY = 100000;
   private static final Log log = LogFactory.getLog(StreamLibContainer.class);
   private final String cacheName;
   private final String address;
   private final Map<Stat, StreamSummary<Object>> streamSummaryEnumMap;
   private final Map<Stat, Lock> lockMap;
   private volatile int capacity = 1000;
   private volatile boolean active = false;

   public StreamLibContainer(String cacheName, String address) {
      this.cacheName = cacheName;
      this.address = address;
      streamSummaryEnumMap = Collections.synchronizedMap(new EnumMap<Stat, StreamSummary<Object>>(Stat.class));
      lockMap = new EnumMap<Stat, Lock>(Stat.class);

      for (Stat stat : Stat.values()) {
         lockMap.put(stat, new ReentrantLock());
      }

      resetAll(1);
   }

   public static StreamLibContainer getOrCreateStreamLibContainer(Cache cache) {
      ComponentRegistry componentRegistry = cache.getAdvancedCache().getComponentRegistry();
      StreamLibContainer streamLibContainer = componentRegistry.getComponent(StreamLibContainer.class);
      if (streamLibContainer == null) {
         String cacheName = cache.getName();
         String address = String.valueOf(cache.getCacheManager().getAddress()); 
         componentRegistry.registerComponent(new StreamLibContainer(cacheName, address), StreamLibContainer.class);
      }
      return componentRegistry.getComponent(StreamLibContainer.class);
   }

   public boolean isActive() {
      return active;
   }

   public void setActive(boolean active) {
      if (!this.active && active) {
         resetAll(capacity);
      } else if (!active) {
         resetAll(1);
      }
      this.active = active;
   }

   public int getCapacity() {
      return capacity;
   }

   public void setCapacity(int capacity) {
      if (capacity <= 0) {
         this.capacity = 1;
      } else {
         this.capacity = capacity;
      }
   }

   public void addGet(Object key, boolean remote) {
      if (!isActive()) {
         return;
      }
      syncOffer(remote ? Stat.REMOTE_GET : Stat.LOCAL_GET, key);
   }

   public void addPut(Object key, boolean remote) {
      if (!isActive()) {
         return;
      }

      syncOffer(remote ? Stat.REMOTE_PUT : Stat.LOCAL_PUT, key);
   }

   public void addLockInformation(Object key, boolean contention, boolean abort) {
      if (!isActive()) {
         return;
      }

      syncOffer(Stat.MOST_LOCKED_KEYS, key);

      if (contention) {
         syncOffer(Stat.MOST_CONTENDED_KEYS, key);
      }
      if (abort) {
         syncOffer(Stat.MOST_FAILED_KEYS, key);
      }
   }

   public void addWriteSkewFailed(Object key) {
      syncOffer(Stat.MOST_WRITE_SKEW_FAILED_KEYS, key);
   }

   public Map<Object, Long> getTopKFrom(Stat stat) {
      return getTopKFrom(stat, capacity);
   }

   public Map<Object, Long> getTopKFrom(Stat stat, int topK) {
      try {
         lockMap.get(stat).lock();
         return getStatsFrom(streamSummaryEnumMap.get(stat), topK);
      } finally {
         lockMap.get(stat).unlock();
      }

   }

   private Map<Object, Long> getStatsFrom(StreamSummary<Object> ss, int topK) {
      List<Counter<Object>> counters = ss.topK(topK <= 0 ? 1 : topK);
      Map<Object, Long> results = new HashMap<Object, Long>(topK);

      for (Counter<Object> c : counters) {
         results.put(c.getItem(), c.getCount());
      }

      return results;
   }

   public void resetAll() {
      resetAll(capacity);
   }

   public void resetStat(Stat stat) {
      resetStat(stat, capacity);
   }

   private void resetStat(Stat stat, int customCapacity) {
      try {
         lockMap.get(stat).lock();
         streamSummaryEnumMap.put(stat, createNewStreamSummary(customCapacity));
      } finally {
         lockMap.get(stat).unlock();
      }
   }

   private StreamSummary<Object> createNewStreamSummary(int customCapacity) {
      return new StreamSummary<Object>(Math.min(MAX_CAPACITY, customCapacity));
   }

   private void resetAll(int customCapacity) {
      for (Stat stat : Stat.values()) {
         resetStat(stat, customCapacity);
      }
   }

   private void syncOffer(final Stat stat, Object key) {
      try {
         lockMap.get(stat).lock();
         if (log.isTraceEnabled()) {
            log.tracef("Offer key=%s to stat=%s in %s", key, stat, this);
         }
         streamSummaryEnumMap.get(stat).offer(key);
      } finally {
         lockMap.get(stat).unlock();
      }
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      StreamLibContainer that = (StreamLibContainer) o;

      return !(address != null ? !address.equals(that.address) : that.address != null) && !(cacheName != null ? !cacheName.equals(that.cacheName) : that.cacheName != null);

   }

   @Override
   public int hashCode() {
      int result = cacheName != null ? cacheName.hashCode() : 0;
      result = 31 * result + (address != null ? address.hashCode() : 0);
      return result;
   }

   @Override
   public String toString() {
      return "StreamLibContainer{" +
            "cacheName='" + cacheName + '\'' +
            ", address='" + address + '\'' +
            '}';
   }

   public static enum Stat {
      REMOTE_GET,
      LOCAL_GET,
      REMOTE_PUT,
      LOCAL_PUT,

      MOST_LOCKED_KEYS,
      MOST_CONTENDED_KEYS,
      MOST_FAILED_KEYS,
      MOST_WRITE_SKEW_FAILED_KEYS
   }
}
