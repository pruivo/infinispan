package org.infinispan.remoting.transport.jgroups;

import org.jgroups.util.ExtendedUUID;
import org.jgroups.util.UUID;
import org.jgroups.util.Util;

import java.util.Arrays;

/**
 * @author Dan Berindei
 * @since 9.0
 */
public class TopologyUUID {
   private static final byte[] SITE_KEY = new byte[]{'s'};
   private static final byte[] RACK_KEY = new byte[]{'r'};
   private static final byte[] MACHINE_KEY = new byte[]{'m'};

   private TopologyUUID() {
   }

   public static ExtendedUUID randomUUID(String name, String siteId, String rackId, String machineId) {
      ExtendedUUID uuid = ExtendedUUID.randomUUID(name);
      if (name != null) {
         UUID.add(uuid, name);
      }
      addId(uuid, SITE_KEY, siteId);
      addId(uuid, RACK_KEY, rackId);
      addId(uuid, MACHINE_KEY, machineId);
      return uuid;
   }

   private static void addId(ExtendedUUID uuid, byte[] key, String stringValue) {
      if (stringValue != null) {
         uuid.put(key, Util.stringToBytes(stringValue));
      }
   }

   public static boolean matches(ExtendedUUID uuid, String siteId, String rackId, String machineId) {
      return checkId(uuid, SITE_KEY, siteId) &&
            checkId(uuid, RACK_KEY, rackId) &&
            checkId(uuid, MACHINE_KEY, machineId);
   }

   private static boolean checkId(ExtendedUUID uuid, byte[] key, String stringValue) {
      return Arrays.equals(Util.stringToBytes(stringValue), uuid.get(key));
   }
   
   public static String getSiteId(ExtendedUUID uuid) {
      return Util.bytesToString(uuid.get(SITE_KEY));
   }

   public static String getRackId(ExtendedUUID uuid) {
      return Util.bytesToString(uuid.get(RACK_KEY));
   }

   public static String getMachineId(ExtendedUUID uuid) {
      return Util.bytesToString(uuid.get(MACHINE_KEY));
   }
}
