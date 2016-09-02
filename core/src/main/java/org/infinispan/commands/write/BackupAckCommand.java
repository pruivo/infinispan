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
public class BackupAckCommand extends BaseRpcCommand {

   public static final byte COMMAND_ID = 59;
   private CommandInvocationId commandInvocationId;
   private Object previousValue;
   private CommandAckCollector commandAckCollector;

   public BackupAckCommand(ByteString cacheName) {
      super(cacheName);
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      commandAckCollector.ack(commandInvocationId, getOrigin(), previousValue);
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
      output.writeObject(previousValue);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      commandInvocationId = CommandInvocationId.readFrom(input);
      previousValue = input.readObject();
   }

   public void setCommandAckCollector(CommandAckCollector commandAckCollector) {
      this.commandAckCollector = commandAckCollector;
   }

   public void setCommandInvocationId(CommandInvocationId commandInvocationId) {
      this.commandInvocationId = commandInvocationId;
   }

   public void setPreviousValue(Object previousValue) {
      this.previousValue = previousValue;
   }

   @Override
   public String toString() {
      return "BackupAckCommand{" +
            "commandInvocationId=" + commandInvocationId +
            ",cacheName='" + cacheName + '\'' +
            '}';
   }
}
