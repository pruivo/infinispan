package org.infinispan.commands.write;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.marshall.MarshallUtil;
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

   public static final byte COMMAND_ID = 60;
   private static final Type[] CACHED_TYPE = Type.values();
   private CommandInvocationId commandInvocationId;
   private Object returnValue;
   private Type type;
   private CommandAckCollector commandAckCollector;

   public PrimaryAckCommand(ByteString cacheName) {
      super(cacheName);
   }

   private static Type valueOf(int index) {
      return CACHED_TYPE[index];
   }

   private static boolean isSuccessful(Type type) {
      switch (type) {
         case SUCCESS_WITH_BOOL_RETURN_VALUE:
         case SUCCESS_WITH_RETURN_VALUE:
         case SUCCESS_WITHOUT_RETURN_VALUE:
            return true;
      }
      return false;
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      commandAckCollector.primaryAck(commandInvocationId, returnValue, isSuccessful(type));
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

   public void initWithReturnValue(boolean success, Object returnValue) {
      this.returnValue = returnValue;
      if (success) {
         type = Type.SUCCESS_WITH_RETURN_VALUE;
      } else {
         type = Type.UNSUCCESSFUL_WITH_RETURN_VALUE;
      }
   }

   public void initWithBoolReturnValue(boolean success, boolean returnValue) {
      this.returnValue = returnValue;
      if (success) {
         type = Type.SUCCESS_WITH_BOOL_RETURN_VALUE;
      } else {
         type = Type.UNSUCCESSFUL_WITH_BOOL_RETURN_VALUE;
      }
   }

   public void initWithoutReturnValue(boolean success) {
      if (success) {
         type = Type.SUCCESS_WITHOUT_RETURN_VALUE;
      } else {
         type = Type.UNSUCCESSFUL_WITHOUT_RETURN_VALUE;
      }
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      CommandInvocationId.writeTo(output, commandInvocationId);
      MarshallUtil.marshallEnum(type, output);
      switch (type) {
         case SUCCESS_WITH_RETURN_VALUE:
         case UNSUCCESSFUL_WITH_RETURN_VALUE:
            output.writeObject(returnValue);
            break;
         case SUCCESS_WITH_BOOL_RETURN_VALUE:
         case UNSUCCESSFUL_WITH_BOOL_RETURN_VALUE:
            output.writeBoolean((Boolean) returnValue);
            break;
      }
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      commandInvocationId = CommandInvocationId.readFrom(input);
      type = MarshallUtil.unmarshallEnum(input, PrimaryAckCommand::valueOf);
      assert type != null;
      switch (type) {
         case SUCCESS_WITH_RETURN_VALUE:
         case UNSUCCESSFUL_WITH_RETURN_VALUE:
            returnValue = input.readObject();
            break;
         case SUCCESS_WITH_BOOL_RETURN_VALUE:
         case UNSUCCESSFUL_WITH_BOOL_RETURN_VALUE:
            returnValue = input.readBoolean();
            break;
      }
   }

   public void setCommandAckCollector(CommandAckCollector commandAckCollector) {
      this.commandAckCollector = commandAckCollector;
   }

   public void setCommandInvocationId(CommandInvocationId commandInvocationId) {
      this.commandInvocationId = commandInvocationId;
   }

   @Override
   public String toString() {
      return "PrimaryAckCommand{" +
            "commandInvocationId=" + commandInvocationId +
            ", returnValue=" + returnValue +
            ", type=" + type +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      PrimaryAckCommand that = (PrimaryAckCommand) o;

      return type == that.type &&
            commandInvocationId.equals(that.commandInvocationId) &&
            (returnValue != null ? returnValue.equals(that.returnValue) : that.returnValue == null);

   }

   @Override
   public int hashCode() {
      int result = commandInvocationId.hashCode();
      result = 31 * result + (returnValue != null ? returnValue.hashCode() : 0);
      result = 31 * result + type.hashCode();
      return result;
   }

   private enum Type {
      SUCCESS_WITH_RETURN_VALUE,
      SUCCESS_WITH_BOOL_RETURN_VALUE,
      SUCCESS_WITHOUT_RETURN_VALUE,
      UNSUCCESSFUL_WITH_RETURN_VALUE,
      UNSUCCESSFUL_WITH_BOOL_RETURN_VALUE,
      UNSUCCESSFUL_WITHOUT_RETURN_VALUE
   }
}
