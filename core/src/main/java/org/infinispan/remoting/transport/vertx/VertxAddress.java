package org.infinispan.remoting.transport.vertx;

import java.util.UUID;

import org.infinispan.remoting.transport.Address;

public class VertxAddress implements Address {

   private final String uuid;
   private final String host;
   private final int port;

   private VertxAddress(String uuid, String host, int port) {
      this.uuid = uuid;
      this.host = host;
      this.port = port;
   }


   @Override
   public int compareTo(Address o) {
      if (o instanceof VertxAddress) {
         var addr = (VertxAddress) o;
         var cmp = uuid.compareTo(addr.uuid);
         if (cmp != 0) {
            return cmp;
         }
         cmp = host.compareTo(addr.host);
         if (cmp != 0) {
            return cmp;
         }
         return Integer.compare(port, addr.port);
      }
      throw new IllegalArgumentException("Unable to compare with " + o);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      VertxAddress that = (VertxAddress) o;

      if (port != that.port) return false;
      if (!uuid.equals(that.uuid)) return false;
      return host.equals(that.host);
   }

   @Override
   public int hashCode() {
      int result = uuid.hashCode();
      result = 31 * result + host.hashCode();
      result = 31 * result + port;
      return result;
   }

   public static VertxAddress createRandom(String host, int port) {
      return new VertxAddress(UUID.randomUUID().toString(), host, port);
   }
}
