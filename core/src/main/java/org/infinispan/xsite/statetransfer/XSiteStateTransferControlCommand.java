package org.infinispan.xsite.statetransfer;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.context.InvocationContext;

/**
 * Command used to control the state transfer between sites.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class XSiteStateTransferControlCommand extends BaseRpcCommand {

   public static final int COMMAND_ID = 28;

   private StateTransferControl control;
   private XSiteStateProvider provider;
   private XSiteStateConsumer consumer;
   private XSiteStateTransferManager stateTransferManager;
   private String siteName;

   public XSiteStateTransferControlCommand(String cacheName, StateTransferControl control, String siteName) {
      super(cacheName);
      this.control = control;
      this.siteName = siteName;
   }

   public XSiteStateTransferControlCommand(String cacheName) {
      super(cacheName);
   }

   public XSiteStateTransferControlCommand() {
      super(null);
   }

   public final void initialize(XSiteStateProvider provider, XSiteStateConsumer consumer,
                                XSiteStateTransferManager stateTransferManager) {
      this.provider = provider;
      this.consumer = consumer;
      this.stateTransferManager = stateTransferManager;
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      switch (control) {
         case START_SEND:
            provider.startStateTransfer(siteName, getOrigin());
            break;
         case START_RECEIVE:
            consumer.startStateTransfer();
            break;
         case FINISH_RECEIVE:
            consumer.endStateTransfer();
            break;
         case FINISH_SEND:
            stateTransferManager.notifyStateTransferFinish(siteName, getOrigin());
            break;
         case CANCEL_SEND:
            provider.cancelStateTransfer(siteName);
            break;
      }
      return null;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      switch (control) {
         case START_SEND:
         case FINISH_SEND:
         case CANCEL_SEND:
            return new Object[]{control.toByte(), siteName};
      }
      return new Object[]{control.toByte()};
   }

   @Override
   public void setParameters(int commandId, Object[] parameters) {
      if (commandId != COMMAND_ID) {
         throw new IllegalArgumentException("CommandId is not valid! (" + commandId + " != " + COMMAND_ID + ")");
      }
      this.control = StateTransferControl.fromByte((Byte) parameters[0]);
      switch (control) {
         case START_SEND:
         case FINISH_SEND:
         case CANCEL_SEND:
            this.siteName = (String) parameters[1];
            break;
      }
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }

   public static enum StateTransferControl {
      START_SEND,
      START_RECEIVE,
      FINISH_SEND,
      FINISH_RECEIVE,
      CANCEL_SEND;

      public final byte toByte() {
         return (byte) ordinal();
      }

      public static StateTransferControl fromByte(byte byteValue) {
         if (byteValue < 0 || byteValue >= values().length) {
            throw new IllegalArgumentException("Byte value '" + byteValue + "' is not valid!");
         }
         return values()[byteValue];
      }
   }
}
