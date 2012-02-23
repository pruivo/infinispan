package org.infinispan.remoting.responses;

import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.transport.Address;

import java.util.Set;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class KeysValidateFilter implements ResponseFilter {

   private final Address localAddress;
   private final Set<Object> keysAwaitingValidation;
   private boolean selfDelivered;

   public KeysValidateFilter(Address localAddress, Set<Object> keysAwaitingValidation) {
      this.localAddress = localAddress;
      this.keysAwaitingValidation = keysAwaitingValidation;
      this.selfDelivered = false;
   }

   @Override
   public boolean isAcceptable(Response response, Address sender) {
      if (response instanceof SuccessfulResponse) {
         Object retVal = ((SuccessfulResponse) response).getResponseValue();
         if (retVal instanceof EntryVersionsMap) {
            keysAwaitingValidation.removeAll(((EntryVersionsMap) retVal).keySet());
         }
      }
      if (sender.equals(localAddress)) {
         selfDelivered = true;
      }
      return true;
   }

   @Override
   public boolean needMoreResponses() {
      return !selfDelivered || !keysAwaitingValidation.isEmpty();
   }
}
