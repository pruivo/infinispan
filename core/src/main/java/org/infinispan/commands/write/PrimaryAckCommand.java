package org.infinispan.commands.write;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.CommandAckCollector;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public class PrimaryAckCommand extends BaseRpcCommand {

   public static final byte COMMAND_ID = 62;
   private CommandInvocationId commandInvocationId;
   private Object returnValue;
   private boolean success;
   private CommandAckCollector commandAckCollector;

   public PrimaryAckCommand(ByteString cacheName) {
      super(cacheName);
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      commandAckCollector.primaryAck(commandInvocationId, returnValue, success);
      return null;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      CommandInvocationId.writeTo(output, commandInvocationId);
      output.writeBoolean(success);
      output.writeObject(returnValue);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      commandInvocationId = CommandInvocationId.readFrom(input);
      success = input.readBoolean();
      returnValue = input.readObject();
   }

   public void setCommandAckCollector(CommandAckCollector commandAckCollector) {
      this.commandAckCollector = commandAckCollector;
   }

   public void setCommandInvocationId(CommandInvocationId commandInvocationId) {
      this.commandInvocationId = commandInvocationId;
   }

   public void setReturnValue(Object returnValue) {
      this.returnValue = returnValue;
   }

   public void setSuccess(boolean success) {
      this.success = success;
   }

   @Override
   public String toString() {
      return "PrimaryAckCommand{" +
            "commandInvocationId=" + commandInvocationId +
            ", returnValue=" + returnValue +
            ", success=" + success +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      PrimaryAckCommand that = (PrimaryAckCommand) o;

      return success == that.success &&
            commandInvocationId.equals(that.commandInvocationId) &&
            (returnValue != null ? returnValue.equals(that.returnValue) : that.returnValue == null);

   }

   @Override
   public int hashCode() {
      int result = commandInvocationId.hashCode();
      result = 31 * result + (returnValue != null ? returnValue.hashCode() : 0);
      result = 31 * result + (success ? 1 : 0);
      return result;
   }
}
