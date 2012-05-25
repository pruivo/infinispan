package org.infinispan.statetransfer;

import java.io.Serializable;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class ConfigurationState implements Serializable {

   private final String protocolName;
   private final long epoch;

   public ConfigurationState(String protocolName, long epoch) {
      this.protocolName = protocolName;
      this.epoch = epoch;
   }

   public String getProtocolName() {
      return protocolName;
   }

   public long getEpoch() {
      return epoch;
   }

   @Override
   public String toString() {
      return "ConfigurationState{" +
            ", protocolName='" + protocolName + '\'' +
            ", epoch=" + epoch +
            '}';
   }
}
