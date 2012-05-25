package org.infinispan.commands.remote;

import org.infinispan.context.InvocationContext;
import org.infinispan.reconfigurableprotocol.manager.ProtocolManager;
import org.infinispan.reconfigurableprotocol.manager.ReconfigurableReplicationManager;
import org.infinispan.statetransfer.ConfigurationState;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class ConfigurationStateCommand extends BaseRpcCommand {

   public static final byte COMMAND_ID = 105;
   private static final Object[] EMPTY_ARRAY = new Object[0];
   private ReconfigurableReplicationManager reconfigurableReplicationManager;

   public ConfigurationStateCommand(String cacheName) {
      super(cacheName);
   }

   public final void initialize(ReconfigurableReplicationManager reconfigurableReplicationManager) {
      this.reconfigurableReplicationManager = reconfigurableReplicationManager;
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      ProtocolManager.CurrentProtocolInfo protocolName = reconfigurableReplicationManager.getProtocolManager().getCurrentProtocolInfo();
      return new ConfigurationState(protocolName.getCurrent().getUniqueProtocolName(), protocolName.getEpoch());
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      return EMPTY_ARRAY;
   }

   @Override
   public void setParameters(int commandId, Object[] parameters) {
      //nothing
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }
}
