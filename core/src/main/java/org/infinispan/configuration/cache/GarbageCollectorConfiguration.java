package org.infinispan.configuration.cache;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class GarbageCollectorConfiguration {

   private final boolean enabled;
   private final int transactionThreshold;
   private final int versionGCMaxIdle;
   private final int l1GCInterval;
   private final int viewGCBackOff;

   public GarbageCollectorConfiguration(boolean enabled, int transactionThreshold, int versionGCMaxIdle,
                                        int l1GCInterval, int viewGCBackOff) {
      this.enabled = enabled;
      this.transactionThreshold = transactionThreshold;
      this.versionGCMaxIdle = versionGCMaxIdle;
      this.l1GCInterval = l1GCInterval;
      this.viewGCBackOff = viewGCBackOff;
   }

   public boolean enabled() {
      return enabled;
   }

   public int transactionThreshold() {
      return transactionThreshold;
   }

   public int versionGCMaxIdle() {
      return versionGCMaxIdle;
   }

   public int l1GCInterval() {
      return l1GCInterval;
   }

   public int viewGCBackOff() {
      return viewGCBackOff;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      GarbageCollectorConfiguration that = (GarbageCollectorConfiguration) o;

      return enabled == that.enabled &&
            l1GCInterval == that.l1GCInterval &&
            versionGCMaxIdle == that.versionGCMaxIdle &&
            transactionThreshold == that.transactionThreshold &&
            viewGCBackOff == that.viewGCBackOff;

   }

   @Override
   public int hashCode() {
      int result = (enabled ? 1 : 0);
      result = 31 * result + transactionThreshold;
      result = 31 * result + versionGCMaxIdle;
      result = 31 * result + l1GCInterval;
      result = 31 * result + viewGCBackOff;
      return result;
   }

   @Override
   public String toString() {
      return "GarbageCollectorConfiguration{" +
            "enabled=" + enabled +
            ", transactionThreshold=" + transactionThreshold +
            ", versionGCMaxIdle=" + versionGCMaxIdle +
            ", l1GCInterval=" + l1GCInterval +
            ", viewGCBackOff=" + viewGCBackOff +
            '}';
   }
}
