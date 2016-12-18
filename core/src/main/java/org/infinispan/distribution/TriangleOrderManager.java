package org.infinispan.distribution;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.infinispan.topology.CacheTopology;

import net.jcip.annotations.GuardedBy;

/**
 * //TODO document this!
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class TriangleOrderManager {

   private static final long INVALID_TOPOLOGY = -1;
   private final Map<Integer, SenderSequencer> senderSegmentMap;
   private final Map<Integer, ReceiverSequencer> receiverSegmentMap;
   private volatile CacheTopology currentCacheTopology;

   public TriangleOrderManager() {
      senderSegmentMap = new ConcurrentHashMap<>();
      receiverSegmentMap = new ConcurrentHashMap<>();
   }

   public int calculateSegmentId(Object key) {
      return currentCacheTopology.getWriteConsistentHash().getSegment(key);
   }

   public long next(int segmentId, final int commandTopologyId) {
      final CacheTopology cacheTopology = currentCacheTopology;
      return commandTopologyId == cacheTopology.getTopologyId() ?
            getNext(segmentId, commandTopologyId) :
            INVALID_TOPOLOGY;
   }

   public Map<Integer, Long> next(Collection<Integer> segmentsIds, final int commandTopologyId) {
      final CacheTopology cacheTopology = currentCacheTopology;
      if (commandTopologyId != cacheTopology.getTopologyId()) {
         return null;
      }
      List<Integer> orderedSegmentsIds = new ArrayList<>(segmentsIds);
      Collections.sort(orderedSegmentsIds);
      Map<Integer, Long> map = new HashMap<>();
      orderedSegmentsIds.forEach(segmentId -> map.put(segmentId, getNext(segmentId, commandTopologyId)));
      return map.containsValue(INVALID_TOPOLOGY) ? null : map;
   }

   public boolean isNext(int segmentId, int commandTopologyId, long sequenceNumber) {
      final CacheTopology cacheTopology = currentCacheTopology;
      final int topologyId = cacheTopology.getTopologyId();
      return commandTopologyId < topologyId ||
            (commandTopologyId == topologyId && checkIfNext(segmentId, commandTopologyId, sequenceNumber));
   }

   public boolean isNext(Map<Integer, Long> sequenceNumbers, int commandTopologyId) {
      final CacheTopology cacheTopology = currentCacheTopology;
      final int topologyId = cacheTopology.getTopologyId();
      if (commandTopologyId < topologyId) {
         return true;
      }
      if (commandTopologyId == topologyId) {
         boolean result = true;
         for (Map.Entry<Integer, Long> entry : sequenceNumbers.entrySet()) {
            result = result && checkIfNext(entry.getKey(), topologyId, entry.getValue());
         }
         return result;
      }
      return false;
   }

   public void markDelivered(int segmentId, int commandTopologyId, long sequenceNumber) {
      final CacheTopology cacheTopology = currentCacheTopology;
      final int topologyId = cacheTopology.getTopologyId();
      deliver(segmentId, commandTopologyId, sequenceNumber);
   }

   public void updateCacheTopology(CacheTopology newCacheTopology) {
      this.currentCacheTopology = newCacheTopology;
   }

   private void deliver(int segmentId, int commandTopologyId, long sequenceNumber) {
      ReceiverSequencer sequencer = receiverSegmentMap.get(segmentId);
      if (sequencer != null) {
         sequencer.deliver(commandTopologyId, sequenceNumber);
      }
   }

   private long getNext(int segmentId, int topologyId) {
      return senderSegmentMap.computeIfAbsent(segmentId, integer -> new SenderSequencer(topologyId)).next(topologyId);
   }

   private boolean checkIfNext(int segmentId, int topologyId, long sequenceNumber) {
      return receiverSegmentMap.computeIfAbsent(segmentId, integer -> new ReceiverSequencer(topologyId))
            .isNext(topologyId, sequenceNumber);
   }

   private class SenderSequencer {
      @GuardedBy("this")
      private int topologyId;
      @GuardedBy("this")
      private long sequenceNumber = 1;

      private SenderSequencer(int topologyId) {
         setTopologyId(topologyId);
      }

      private synchronized long next(int commandTopologyId) {
         if (topologyId == commandTopologyId) {
            return sequenceNumber++;
         } else if (topologyId < commandTopologyId) {
            //update topology. this command will be the first
            setTopologyId(commandTopologyId);
            return sequenceNumber++;
         } else {
            //this topology is higher than the command topology id.
            //another topology was installed. this command will fail with OutdatedTopologyException.
            return INVALID_TOPOLOGY;
         }
      }

      private void setTopologyId(int topologyId) {
         this.topologyId = topologyId;
         this.sequenceNumber = 1;
      }
   }

   private class ReceiverSequencer {
      @GuardedBy("this")
      private int topologyId;
      @GuardedBy("this")
      private long nextSequenceNumber = 1;

      private ReceiverSequencer(int topologyId) {
         setTopologyId(topologyId);
      }

      private synchronized void deliver(int commandTopologyId, long sequenceNumber) {
         if (topologyId == commandTopologyId && nextSequenceNumber == sequenceNumber) {
            nextSequenceNumber++;
         }
      }

      private synchronized boolean isNext(int commandTopologyId, long sequenceNumber) {
         if (topologyId == commandTopologyId) {
            return nextSequenceNumber == sequenceNumber;
         } else if (topologyId < commandTopologyId) {
            //update topology. this command will be the first
            setTopologyId(commandTopologyId);
            return nextSequenceNumber == sequenceNumber;
         } else {
            //this topology is higher than the command topology id.
            //another topology was installed. this command will fail with OutdatedTopologyException.
            return true;
         }
      }

      private void setTopologyId(int topologyId) {
         this.topologyId = topologyId;
         this.nextSequenceNumber = 1;
      }
   }
}
