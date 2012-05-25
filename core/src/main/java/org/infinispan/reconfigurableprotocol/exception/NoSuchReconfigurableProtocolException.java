package org.infinispan.reconfigurableprotocol.exception;

/**
 * Exception thrown when you try to change to a non-existent protocol
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class NoSuchReconfigurableProtocolException extends Exception {

   public NoSuchReconfigurableProtocolException(String protocolId) {
      super("The protocol " + protocolId + " does not exist");
   }
}
