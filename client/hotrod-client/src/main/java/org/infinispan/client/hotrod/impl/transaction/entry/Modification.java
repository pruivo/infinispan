package org.infinispan.client.hotrod.impl.transaction.entry;

import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.Transport;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public class Modification {

   private final byte[] key;
   private final byte[] value;
   private final long versionRead;
   private final long lifespan;
   private final long maxIdle;
   private final TimeUnit lifespanTimeUnit;
   private final TimeUnit maxIdleTimeUnit;
   private final byte control;

   public Modification(byte[] key, byte[] value, long versionRead, long lifespan, long maxIdle,
         TimeUnit lifespanTimeUnit, TimeUnit maxIdleTimeUnit, byte control) {
      this.key = key;
      this.value = value;
      this.versionRead = versionRead;
      this.lifespan = lifespan;
      this.maxIdle = maxIdle;
      this.lifespanTimeUnit = lifespanTimeUnit;
      this.maxIdleTimeUnit = maxIdleTimeUnit;
      this.control = control;
   }


   public void writeTo(Transport transport, Codec codec) {
      transport.writeArray(key);
      transport.writeByte(control);
      if (!ControlByte.NON_EXISTING.hasFlag(control) && !ControlByte.NOT_READ.hasFlag(control)) {
         transport.writeLong(versionRead);
      }
      if (ControlByte.REMOVE_OP.hasFlag(control)) {
         return;
      }
      codec.writeExpirationParams(transport, lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit);
      transport.writeArray(value);

   }

}
