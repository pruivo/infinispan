package org.infinispan.util.concurrent.locks;

import org.infinispan.atomic.DeltaCompositeKey;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;

import java.util.Collection;
import java.util.Objects;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public class LockUtil {

   private LockUtil() {
   }

   public static void filterByLockOwnership(Collection<?> keys, Collection<Object> primary, Collection<Object> backup,
                                            ClusteringDependentLogic clusteringDependentLogic) {
      Objects.requireNonNull(clusteringDependentLogic, "Cluster Dependent Logic shouldn't be null.");

      if (keys == null || keys.isEmpty()) {
         return;
      }

      for (Object key : keys) {
         Object keyToCheck = key instanceof DeltaCompositeKey ?
               ((DeltaCompositeKey) key).getDeltaAwareValueKey() :
               key;
         switch (getLockOwnership(keyToCheck, clusteringDependentLogic)) {
            case PRIMARY:
               if (primary != null) {
                  primary.add(key);
               }
               break;
            case BACKUP:
               if (backup != null) {
                  backup.add(key);
               }
               break;
         }
      }
   }

   public static LockOwnership getLockOwnership(Object key, ClusteringDependentLogic clusteringDependentLogic) {
      if (clusteringDependentLogic.localNodeIsPrimaryOwner(key)) {
         return LockOwnership.PRIMARY;
      } else if (clusteringDependentLogic.localNodeIsOwner(key)) {
         return LockOwnership.BACKUP;
      } else {
         return LockOwnership.NO_OWNER;
      }
   }


   public enum LockOwnership {
      NO_OWNER, PRIMARY, BACKUP
   }

}
