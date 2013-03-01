package org.infinispan.dataplacement;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

/**
 * Contains the information of the remote and local accesses for a member. This information is sent to the primary owner
 * in orde to calculate the best placement for the objects
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class ObjectRequest implements Serializable {

   private final Map<Object, Long> remoteAccesses;
   private final Map<Object, Long> localAccesses;

   public ObjectRequest(Map<Object, Long> remoteAccesses, Map<Object, Long> localAccesses) {
      this.remoteAccesses = remoteAccesses;
      this.localAccesses = localAccesses;
   }

   public Map<Object, Long> getRemoteAccesses() {
      return remoteAccesses == null ? Collections.<Object, Long>emptyMap() : remoteAccesses;
   }

   public Map<Object, Long> getLocalAccesses() {
      return localAccesses == null ? Collections.<Object, Long>emptyMap() : localAccesses;
   }

   @Override
   public String toString() {
      return "ObjectRequest{" +
            "remoteAccesses=" + (remoteAccesses == null ? 0 : remoteAccesses.size()) +
            ", localAccesses=" + (localAccesses == null ? 0 : localAccesses.size()) +
            '}';
   }

   public String toString(boolean detailed) {
      if (detailed) {
         return "ObjectRequest{" +
               "remoteAccesses=" + remoteAccesses +
               ", localAccesses=" + localAccesses +
               '}';
      }
      return toString();
   }
}
