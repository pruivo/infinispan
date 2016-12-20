package org.infinispan.distribution;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.topology.CacheTopology;

import net.jcip.annotations.GuardedBy;

/**
 * //TODO document this!
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class TriangleOrderManager {

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
      if (commandTopologyId != cacheTopology.getTopologyId()) {
         throw OutdatedTopologyException.getCachedInstance();
      }
      return getNext(segmentId, commandTopologyId);
   }

   public void next(Map<Integer, Long> segmentsIds, final int commandTopologyId) {
      final CacheTopology cacheTopology = currentCacheTopology;
      if (commandTopologyId != cacheTopology.getTopologyId()) {
         throw OutdatedTopologyException.getCachedInstance();
      }
      for (Map.Entry<Integer, Long> entry : segmentsIds.entrySet()) {
         entry.setValue(getNext(entry.getKey(), commandTopologyId));
      }
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
      return commandTopologyId < topologyId ||
            (commandTopologyId == topologyId && checkAllEntries(sequenceNumbers, commandTopologyId));
   }

   public void markDelivered(int segmentId, int commandTopologyId, long sequenceNumber) {
      ReceiverSequencer sequencer = receiverSegmentMap.get(segmentId);
      if (sequencer != null) {
         sequencer.deliver(commandTopologyId, sequenceNumber);
      }
   }

   public void updateCacheTopology(CacheTopology newCacheTopology) {
      this.currentCacheTopology = newCacheTopology;
   }

   private long getNext(int segmentId, int topologyId) {
      return senderSegmentMap.computeIfAbsent(segmentId, integer -> new SenderSequencer(topologyId)).next(topologyId);
   }

   private boolean checkIfNext(int segmentId, int topologyId, long sequenceNumber) {
      return receiverSegmentMap.computeIfAbsent(segmentId, integer -> new ReceiverSequencer(topologyId))
            .isNext(topologyId, sequenceNumber);
   }

   private boolean checkAllEntries(Map<Integer, Long> sequenceNumbers, int commandTopologyId) {
      Iterator<Map.Entry<Integer, Long>> iterator = sequenceNumbers.entrySet().iterator();
      boolean result = true;
      while (result && iterator.hasNext()) {
         Map.Entry<Integer, Long> entry = iterator.next();
         result = checkIfNext(entry.getKey(), commandTopologyId, entry.getValue());
      }
      return result;
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
            throw OutdatedTopologyException.getCachedInstance();
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
