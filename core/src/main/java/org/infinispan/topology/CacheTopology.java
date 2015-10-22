package org.infinispan.topology;

import org.infinispan.commons.marshall.InstanceReusingAdvancedExternalizer;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.ConsistentHashFactory;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.distribution.group.PartitionerConsistentHash;
import org.infinispan.marshall.core.Ids;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.infinispan.commons.util.InfinispanCollections.immutableRetainAll;

/**
 * The status of a cache from a distribution/state transfer point of view.
 * <p/>
 * The pending CH can be {@code null} if we don't have a state transfer in progress.
 * <p/>
 * The {@code topologyId} is incremented every time the topology changes (e.g. a member leaves, state transfer
 * starts or ends).
 * The {@code rebalanceId} is not modified when the consistent hashes are updated without requiring state
 * transfer (e.g. when a member leaves).
 *
 * @author Dan Berindei
 * @author Pedro Ruivo
 * @since 5.2
 */
public class CacheTopology {

   private static Log log = LogFactory.getLog(CacheTopology.class);
   private static final boolean trace = log.isTraceEnabled();

   private final int topologyId;
   private final int rebalanceId;
   private final ConsistentHash readCH;
   private final ConsistentHash writeCH;
   private final List<Address> actualMembers;
   private final boolean stableTopology;

   public CacheTopology(int topologyId, int rebalanceId, ConsistentHash readCH, ConsistentHash writeCH,
                        List<Address> actualMembers, boolean stableTopology) {
      Objects.requireNonNull(readCH, "Read Consistent Hash must be non null.");
      Objects.requireNonNull(writeCH, "Write Consistent Hash must be non null.");
      if (!writeCH.getMembers().containsAll(readCH.getMembers())) {
         throw new IllegalArgumentException("A cache topology's pending consistent hash must " +
               "contain all the current consistent hash's members");
      }
      this.topologyId = topologyId;
      this.rebalanceId = rebalanceId;
      this.readCH = readCH;
      this.writeCH = writeCH;
      this.actualMembers = actualMembers;
      this.stableTopology = stableTopology;
   }

   public int getTopologyId() {
      return topologyId;
   }

   /**
    * @return the {@link org.infinispan.distribution.ch.ConsistentHash} to read. It returns the same instance as {@link
    * #getWriteConsistentHash()} when no rebalance is in progress.
    */
   public ConsistentHash getReadConsistentHash() {
      return readCH;
   }

   /**
    * @return the {@link org.infinispan.distribution.ch.ConsistentHash} to write. It returns the same instance as {@link
    * #getReadConsistentHash()} when no rebalance is in progress.
    */
   public ConsistentHash getWriteConsistentHash() {
      return writeCH;
   }

   /**
    * The id of the latest started rebalance.
    */
   public int getRebalanceId() {
      return rebalanceId;
   }

   /**
    * @return The nodes that are members in write consistent hash
    * @see {@link #getActualMembers()}
    */
   public List<Address> getMembers() {
      return writeCH.getMembers();
   }

   /**
    * @return The nodes that are active members of the cache. It should be equal to {@link #getMembers()} when the
    *    cache is available, and a strict subset if the cache is in degraded mode.
    * @see org.infinispan.partitionhandling.AvailabilityMode
    */
   public List<Address> getActualMembers() {
      return actualMembers;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      CacheTopology that = (CacheTopology) o;

      if (topologyId != that.topologyId) return false;
      if (rebalanceId != that.rebalanceId) return false;
      if (!readCH.equals(that.readCH)) return false;
      if (!writeCH.equals(that.writeCH)) return false;
      if (actualMembers != null ? !actualMembers.equals(that.actualMembers) : that.actualMembers != null) return false;
      if (stableTopology != that.stableTopology) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = topologyId;
      result = 31 * result + rebalanceId;
      result = 31 * result + readCH.hashCode();
      result = 31 * result + writeCH.hashCode();
      result = 31 * result + (actualMembers != null ? actualMembers.hashCode() : 0);
      result = 31 * result + (stableTopology ? 31 : 0);
      return result;
   }

   @Override
   public String toString() {
      return "CacheTopology{" +
            "id=" + topologyId +
            ", rebalanceId=" + rebalanceId +
            ", readCH=" + readCH +
            ", writeCH=" + writeCH +
            ", actualMembers=" + actualMembers +
            '}';
   }

   public final void logRoutingTableInformation() {
      if (trace) {
         log.tracef("Read consistent hash's routing table: %s", readCH.getRoutingTableAsString());
         log.tracef("Write consistent hash's routing table: %s", writeCH.getRoutingTableAsString());
      }
   }

   public final CacheTopology addPartitioner(KeyPartitioner partitioner) {
      if (readCH == writeCH || readCH.equals(writeCH)) {
         ConsistentHash newCH = new PartitionerConsistentHash(readCH, partitioner);
         return new CacheTopology(topologyId, rebalanceId, newCH, newCH, actualMembers, stableTopology);
      }
      return new CacheTopology(topologyId, rebalanceId, new PartitionerConsistentHash(readCH, partitioner),
                               new PartitionerConsistentHash(writeCH, partitioner), actualMembers, stableTopology);
   }

   /**
    * It checks if both consistent hashes are the same.
    * <p/>
    * If no rebalance is in progress, the read and the write consistent hash are the same.
    *
    * @return {@code true} if the read and write consistent hash are the same, {@code false} otherwise.
    */
   public final boolean isStable() {
      return stableTopology;
   }

   public static CacheTopology updateTopologyId(int newTopologyId, CacheTopology from) {
      return new CacheTopology(newTopologyId, from.rebalanceId, from.readCH, from.writeCH, from.actualMembers, from.stableTopology);
   }

   public static CacheTopology updateTopologyIdAndMembers(int newTopologyId, List<Address> newMembers, CacheTopology from) {
      return new CacheTopology(newTopologyId, from.rebalanceId, from.readCH, from.writeCH, newMembers, from.stableTopology);
   }

   public static CacheTopology initialTopology(ConsistentHash consistentHash, List<Address> members) {
      return new CacheTopology(0, 0, consistentHash, consistentHash, members, true);
   }

   public static CacheTopology rebalanceTopology(CacheTopology current, ConsistentHash newWriteCH) {
      return new CacheTopology(current.topologyId + 1, current.rebalanceId + 1, current.readCH, newWriteCH, newWriteCH.getMembers(), false);
   }

   public static CacheTopology updateReadCHTopology(CacheTopology current, ConsistentHash newReadCH) {
      return new CacheTopology(current.topologyId + 1, current.rebalanceId, newReadCH, current.writeCH, current.actualMembers, false);
   }

   public static CacheTopology updateWriteCHTopology(CacheTopology current) {
      return new CacheTopology(current.topologyId + 1, current.rebalanceId, current.readCH, current.readCH, current.actualMembers, true);
   }

   public static CacheTopology resetTopology(CacheTopology from) {
      return new CacheTopology(from.topologyId - 1, from.rebalanceId - 1, from.readCH, from.writeCH, from.actualMembers, from.stableTopology);
   }

   public static CacheTopology pruneInvalidMembers(CacheTopology from, List<Address> expectedMembers, ConsistentHashFactory<ConsistentHash> factory, Map<Address, Float> capacityFactor) {
      ConsistentHash readCH = from.readCH;
      ConsistentHash writeCH = from.writeCH;
      List<Address> actualMembers;
      if (readCH.equals(writeCH)) {
         actualMembers = immutableRetainAll(readCH.getMembers(), expectedMembers);
         readCH = factory.updateMembers(readCH, actualMembers, capacityFactor);
         writeCH = readCH;
      } else {
         actualMembers = immutableRetainAll(writeCH.getMembers(), expectedMembers);
         readCH = factory.updateMembers(readCH, immutableRetainAll(readCH.getMembers(), expectedMembers), capacityFactor);
         writeCH = factory.updateMembers(writeCH, actualMembers, capacityFactor);
      }
      return new CacheTopology(from.topologyId + 1, from.rebalanceId, readCH, writeCH, actualMembers, from.stableTopology);
   }

   public static class Externalizer extends InstanceReusingAdvancedExternalizer<CacheTopology> {
      @Override
      public void doWriteObject(ObjectOutput output, CacheTopology cacheTopology) throws IOException {
         output.writeInt(cacheTopology.topologyId);
         output.writeInt(cacheTopology.rebalanceId);
         output.writeBoolean(cacheTopology.stableTopology);
         boolean sameConsistentHash = cacheTopology.readCH.equals(cacheTopology.writeCH);
         if (sameConsistentHash) {
            output.writeBoolean(true);
            output.writeObject(cacheTopology.readCH);
         } else {
            output.writeBoolean(false);
            output.writeObject(cacheTopology.readCH);
            output.writeObject(cacheTopology.writeCH);
         }
         output.writeObject(cacheTopology.actualMembers);
      }

      @Override
      public CacheTopology doReadObject(ObjectInput unmarshaller) throws IOException, ClassNotFoundException {
         int topologyId = unmarshaller.readInt();
         int rebalanceId = unmarshaller.readInt();
         boolean stableTopology = unmarshaller.readBoolean();
         ConsistentHash readCH;
         ConsistentHash writeCH;
         if (unmarshaller.readBoolean()) {
            readCH = (ConsistentHash) unmarshaller.readObject();
            writeCH = readCH;
         } else {
            readCH = (ConsistentHash) unmarshaller.readObject();
            writeCH = (ConsistentHash) unmarshaller.readObject();
         }
         //noinspection unchecked
         List<Address> actualMembers = (List<Address>) unmarshaller.readObject();
         return new CacheTopology(topologyId, rebalanceId, readCH, writeCH, actualMembers, stableTopology);
      }

      @Override
      public Integer getId() {
         return Ids.CACHE_TOPOLOGY;
      }

      @Override
      public Set<Class<? extends CacheTopology>> getTypeClasses() {
         return Collections.<Class<? extends CacheTopology>>singleton(CacheTopology.class);
      }
   }
}
