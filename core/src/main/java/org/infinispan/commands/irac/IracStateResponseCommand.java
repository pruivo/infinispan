package org.infinispan.commands.irac;

import static org.infinispan.commons.marshall.MarshallUtil.marshallCollection;
import static org.infinispan.commons.marshall.MarshallUtil.unmarshallCollection;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.ByteString;
import org.infinispan.xsite.irac.IracManager;
import org.infinispan.xsite.irac.IracManagerKeyInfo;
import org.infinispan.xsite.irac.IracManagerKeyInfoImpl;

/**
 * The IRAC state for a given key.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public class IracStateResponseCommand implements CacheRpcCommand {

   public static final byte COMMAND_ID = 120;

   private ByteString cacheName;
   private Collection<IracManagerKeyInfo> stateCollection;

   @SuppressWarnings("unused")
   public IracStateResponseCommand() {
   }

   public IracStateResponseCommand(ByteString cacheName) {
      this.cacheName = cacheName;
   }

   public IracStateResponseCommand(ByteString cacheName, int capacity) {
      this(cacheName);
      stateCollection = new ArrayList<>(capacity);
   }

   @Override
   public CompletionStage<?> invokeAsync(ComponentRegistry registry) {
      IracManager manager = registry.getIracManager().running();
      for (IracManagerKeyInfo state : stateCollection) {
         manager.receiveState(state.getSegment(), state.getKey(), state.getOwner());
      }
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
      marshallCollection(stateCollection, output, IracManagerKeyInfoImpl::writeTo);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      stateCollection = unmarshallCollection(input, ArrayList::new, IracManagerKeyInfoImpl::readFrom);
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

   public void add(IracManagerKeyInfo keyInfo) {
      stateCollection.add(keyInfo);
   }

   @Override
   public String toString() {
      return "IracStateResponseCommand{" +
            "cacheName=" + cacheName +
            ", state=" + Util.toStr(stateCollection) +
            '}';
   }

}
