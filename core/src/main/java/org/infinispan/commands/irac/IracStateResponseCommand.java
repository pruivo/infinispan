package org.infinispan.commands.irac;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.InitializableCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.xsite.irac.IracManager;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public class IracStateResponseCommand implements CacheRpcCommand, InitializableCommand {

   public static final byte COMMAND_ID = 88;

   private ByteString cacheName;
   private Object key;
   private Object lockOwner;
   private IracMetadata tombstone;
   private IracManager iracManager;

   @SuppressWarnings("unused")
   public IracStateResponseCommand() {
   }

   public IracStateResponseCommand(ByteString cacheName) {
      this.cacheName = cacheName;
   }

   public IracStateResponseCommand(ByteString cacheName, Object key, Object lockOwner, IracMetadata tombstone) {
      this(cacheName);
      this.key = key;
      this.lockOwner = lockOwner;
      this.tombstone = tombstone;
   }

   @Override
   public CompletableFuture<Object> invokeAsync() throws Throwable {
      iracManager.receiveState(key, lockOwner, tombstone);
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
   public boolean canBlock() {
      return false;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeObject(key);
      boolean cId = lockOwner instanceof CommandInvocationId;
      output.writeBoolean(cId);
      if (cId) {
         CommandInvocationId.writeTo(output, (CommandInvocationId) lockOwner);
      } else {
         output.writeObject(lockOwner);
      }
      boolean nullTombstone = tombstone == null;
      output.writeBoolean(nullTombstone);
      if (!nullTombstone) {
         tombstone.writeTo(output);
      }

   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      this.key = input.readObject();
      if (input.readBoolean()) {
         lockOwner = CommandInvocationId.readFrom(input);
      } else {
         this.lockOwner = input.readObject();
      }
      if (input.readBoolean()) {
         tombstone = null;
      } else {
         tombstone = IracMetadata.readFrom(input);
      }
   }

   @Override
   public String toString() {
      return "IracStateResponseCommand{" +
             "key=" + key +
             ", lockOwner=" + lockOwner +
             ", tombstone=" + tombstone +
             '}';
   }

   @Override
   public void init(ComponentRegistry componentRegistry, boolean isRemote) {
      this.iracManager = componentRegistry.getIracManager().running();
   }


   @Override
   public ByteString getCacheName() {
      return cacheName;
   }

   @Override
   public Address getOrigin() {
      //no-op
      return null;
   }

   @Override
   public void setOrigin(Address origin) {
      //no-op
   }
}
