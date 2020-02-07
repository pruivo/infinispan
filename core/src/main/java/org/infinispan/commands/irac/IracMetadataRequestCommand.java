package org.infinispan.commands.irac;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.InitializableCommand;
import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.container.versioning.irac.IracVersionGenerator;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.ByteString;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public class IracMetadataRequestCommand implements CacheRpcCommand, InitializableCommand, TopologyAffectedCommand {

   public static final byte COMMAND_ID = 86;

   private ByteString cacheName;
   private int segment;
   private int topologyId = -1;

   private IracVersionGenerator iracVersionGenerator;

   @SuppressWarnings("unused")
   public IracMetadataRequestCommand() {
   }

   public IracMetadataRequestCommand(ByteString cacheName) {
      this.cacheName = cacheName;
   }

   public IracMetadataRequestCommand(ByteString cacheName, int segment) {
      this.cacheName = cacheName;
      this.segment = segment;
   }

   @Override
   public ByteString getCacheName() {
      return cacheName;
   }

   @Override
   public CompletableFuture<Object> invokeAsync() throws Throwable {
      return CompletableFuture.completedFuture(iracVersionGenerator.generateNewMetadata(segment));
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public boolean canBlock() {
      return false;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeInt(segment);
      output.writeInt(topologyId);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      this.segment = input.readInt();
      this.topologyId = input.readInt();
   }

   @Override
   public Address getOrigin() {
      //not needed
      return null;
   }

   @Override
   public void setOrigin(Address origin) {
      //no-op
   }

   @Override
   public void init(ComponentRegistry componentRegistry, boolean isRemote) {
      this.iracVersionGenerator = componentRegistry.getIracVersionGenerator().running();
   }

   @Override
   public String toString() {
      return "IracMetadataRequestCommand{" +
             "segment=" + segment +
             '}';
   }

   @Override
   public int getTopologyId() {
      return topologyId;
   }

   @Override
   public void setTopologyId(int topologyId) {
      this.topologyId = topologyId;
   }
}
