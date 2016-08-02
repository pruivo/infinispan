package org.infinispan.remoting.transport.jgroups;

import org.infinispan.commons.marshall.InstanceReusingAdvancedExternalizer;
import org.infinispan.marshall.core.Ids;
import org.infinispan.remoting.transport.TopologyAwareAddress;
import org.jgroups.util.ExtendedUUID;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

/**
 * An encapsulation of a JGroups Address
 *
 * @author Bela Ban
 * @since 5.0
 */
public class JGroupsTopologyAwareAddress extends JGroupsAddress implements TopologyAwareAddress {

   private final ExtendedUUID topologyAddress;


   public JGroupsTopologyAwareAddress(ExtendedUUID address) {
      super(address);
      topologyAddress = address;
   }


   @Override
   public String getSiteId() {
      return TopologyUUID.getSiteId(topologyAddress);
   }

   @Override
   public String getRackId() {
      return TopologyUUID.getRackId(topologyAddress);
   }

   @Override
   public String getMachineId() {
      return TopologyUUID.getMachineId(topologyAddress);
   }


   @Override
   public boolean isSameSite(TopologyAwareAddress addr) {
      return getSiteId() == null ? addr.getSiteId() == null : getSiteId().equals(addr.getSiteId());
   }

   @Override
   public boolean isSameRack(TopologyAwareAddress addr) {
      if (!isSameSite(addr))
         return false;
      return getRackId() == null ? addr.getRackId() == null : getRackId().equals(addr.getRackId());
   }

   @Override
   public boolean isSameMachine(TopologyAwareAddress addr) {
      if (!isSameRack(addr))
         return false;
      return getMachineId() == null ? addr.getMachineId() == null : getMachineId().equals(addr.getMachineId());
   }

   public static final class Externalizer extends InstanceReusingAdvancedExternalizer<JGroupsTopologyAwareAddress> {
      
      public Externalizer() {
         super(false);
      }
      
      @Override
      public void doWriteObject(ObjectOutput output, JGroupsTopologyAwareAddress address) throws IOException {
         try {
            org.jgroups.util.Util.writeAddress(address.address, output);
         } catch (Exception e) {
            throw new IOException(e);
         }
      }

      @Override
      public JGroupsTopologyAwareAddress doReadObject(ObjectInput unmarshaller) throws IOException, ClassNotFoundException {
         try {
            ExtendedUUID jgroupsAddress = (ExtendedUUID) org.jgroups.util.Util.readAddress(unmarshaller);
            return (JGroupsTopologyAwareAddress) JGroupsAddressCache.fromJGroupsAddress(jgroupsAddress);
         } catch (Exception e) {
            throw new IOException(e);
         }
      }

      @Override
      public Set<Class<? extends JGroupsTopologyAwareAddress>> getTypeClasses() {
         return Collections.singleton(JGroupsTopologyAwareAddress.class);
      }

      @Override
      public Integer getId() {
         return Ids.JGROUPS_TOPOLOGY_AWARE_ADDRESS;
      }
   }
}
