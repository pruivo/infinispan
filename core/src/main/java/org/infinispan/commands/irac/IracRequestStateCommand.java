package org.infinispan.commands.irac;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.InitializableCommand;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSetsExternalization;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.xsite.irac.IracManager;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public class IracRequestStateCommand extends BaseRpcCommand implements InitializableCommand {

   public static final byte COMMAND_ID = 87;

   private IntSet segments;
   private IracManager iracManager;

   @SuppressWarnings("unused")
   public IracRequestStateCommand() {
      super(null);
   }

   public IracRequestStateCommand(ByteString cacheName) {
      super(cacheName);
   }

   public IracRequestStateCommand(ByteString cacheName, IntSet segments) {
      super(cacheName);
      this.segments = segments;
   }

   @Override
   public CompletableFuture<Object> invokeAsync() throws Throwable {
      iracManager.requestState(getOrigin(), segments);
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
      IntSetsExternalization.writeTo(output, segments);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      this.segments = IntSetsExternalization.readFrom(input);
   }

   @Override
   public String toString() {
      return "IracUpdateKeyCommand{" +
             "segments=" + segments +
             '}';
   }

   @Override
   public void init(ComponentRegistry componentRegistry, boolean isRemote) {
      this.iracManager = componentRegistry.getIracManager().running();
   }


}
