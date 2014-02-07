package org.infinispan.xsite.statetransfer;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.context.InvocationContext;

import java.util.Arrays;

/**
 * Wraps the state to be sent to another site
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class XSiteStatePushCommand extends BaseRpcCommand {

   public static final byte COMMAND_ID = 33;
   private XSiteState[] chunk;
   private XSiteStateConsumer consumer;

   public XSiteStatePushCommand(String cacheName, XSiteState[] chunk) {
      super(cacheName);
      this.chunk = chunk;
   }

   public void initialize(XSiteStateConsumer consumer) {
      this.consumer = consumer;
   }

   public XSiteStatePushCommand(String cacheName) {
      super(cacheName);
   }

   public XSiteStatePushCommand() {
      super(null);
   }

   public XSiteState[] getChunk() {
      return chunk;
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      consumer.applyState(chunk);
      return null;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      return chunk;
   }

   @Override
   public void setParameters(int commandId, Object[] parameters) {
      if (commandId != COMMAND_ID) {
         throw new IllegalArgumentException("CommandId is not valid! (" + commandId + " != " + COMMAND_ID + ")");
      }
      this.chunk = Arrays.copyOf(parameters, parameters.length, XSiteState[].class);
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }

   @Override
   public boolean canBlock() {
      return true;
   }

   @Override
   public String toString() {
      return "XSiteStatePushCommand{" +
            "cacheName=" + cacheName +
            " (" + chunk.length + " keys)" +
            '}';
   }
}
