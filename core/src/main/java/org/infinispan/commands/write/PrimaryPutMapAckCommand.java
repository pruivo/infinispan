package org.infinispan.commands.write;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.CommandAckCollector;
import org.infinispan.util.concurrent.CompletableFutures;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * A command that represents an acknowledge sent by the primary owner to the originator.
 * <p>
 * The acknowledge signals a successful execution of the the {@link PutMapCommand}. It contains the segments ids of
 * the updated keys and the return value of this primary owner.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class PrimaryPutMapAckCommand extends BaseRpcCommand {

   public static final byte COMMAND_ID = 31;
   private static final Type[] CACHED_TYPE = Type.values();
   private CommandInvocationId commandInvocationId;
   private Map<Object, Object> returnValue = Collections.emptyMap();
   private Collection<Integer> segments = Collections.emptyList();
   private Type type;
   private CommandAckCollector commandAckCollector;
   private int topologyId;

   public PrimaryPutMapAckCommand() {
      super(null);
   }

   public PrimaryPutMapAckCommand(ByteString cacheName) {
      super(cacheName);
   }

   public PrimaryPutMapAckCommand(ByteString cacheName, CommandInvocationId commandInvocationId, int topologyId) {
      super(cacheName);
      this.commandInvocationId = commandInvocationId;
      this.topologyId = topologyId;
   }

   private static Type valueOf(int index) {
      return CACHED_TYPE[index];
   }

   @Override
   public CompletableFuture<Object> invokeAsync() throws Throwable {
      commandAckCollector.putMapPrimaryAck(commandInvocationId, getOrigin(), segments, returnValue, topologyId);
      return CompletableFutures.completedNull();
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
      MarshallUtil.marshallEnum(type, output);
      output.writeInt(topologyId);
      switch (type) {
         case SUCCESS_WITH_RETURN_VALUE:
            MarshallUtil.marshallIntCollection(segments, output);
            MarshallUtil.marshallMap(returnValue, output);
            break;
         case SUCCESS_WITHOUT_RETURN_VALUE:
            MarshallUtil.marshallIntCollection(segments, output);
            break;
      }
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      commandInvocationId = CommandInvocationId.readFrom(input);
      type = MarshallUtil.unmarshallEnum(input, PrimaryPutMapAckCommand::valueOf);
      topologyId = input.readInt();
      assert type != null;
      switch (type) {
         case SUCCESS_WITH_RETURN_VALUE:
            segments = MarshallUtil.unmarshallIntCollection(input, ArrayList::new);
            returnValue = MarshallUtil.unmarshallMap(input, HashMap::new);
            break;
         case SUCCESS_WITHOUT_RETURN_VALUE:
            segments = MarshallUtil.unmarshallIntCollection(input, ArrayList::new);
            break;
      }
   }

   public void initWithReturnValue(Collection<Integer> segments, Map<Object, Object> returnValue) {
      this.returnValue = returnValue;
      this.segments = segments;
      type = Type.SUCCESS_WITH_RETURN_VALUE;
   }

   public void initWithoutReturnValue(Collection<Integer> segments) {
      this.segments = segments;
      type = Type.SUCCESS_WITHOUT_RETURN_VALUE;
   }

   public void setCommandAckCollector(CommandAckCollector commandAckCollector) {
      this.commandAckCollector = commandAckCollector;
   }

   @Override
   public String toString() {
      return "PrimaryPutMapAckCommand{" +
            "commandInvocationId=" + commandInvocationId +
            ", returnValue=" + returnValue +
            ", segments=" + segments +
            ", type=" + type +
            ", topologyId=" + topologyId +
            '}';
   }

   private enum Type {
      SUCCESS_WITH_RETURN_VALUE,
      SUCCESS_WITHOUT_RETURN_VALUE
   }
}
