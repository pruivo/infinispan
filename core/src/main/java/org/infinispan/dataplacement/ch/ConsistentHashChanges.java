package org.infinispan.dataplacement.ch;

import org.infinispan.dataplacement.ClusterObjectLookup;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
public class ConsistentHashChanges {

   private ClusterObjectLookup newMappings;
   private int newReplicationDegree;

   public ConsistentHashChanges() {
      newMappings = null;
      newReplicationDegree = -1;
   }

   public final ClusterObjectLookup getNewMappings() {
      return newMappings;
   }

   public final void setNewMappings(ClusterObjectLookup newMappings) {
      this.newMappings = newMappings;
   }

   public final int getNewReplicationDegree() {
      return newReplicationDegree;
   }

   public final void setNewReplicationDegree(int newReplicationDegree) {
      this.newReplicationDegree = newReplicationDegree;
   }

   public final boolean hasChanges() {
      return newMappings != null || newReplicationDegree != -1;
   }
}
