package org.infinispan.reconfigurableprotocol.exception;

import org.infinispan.reconfigurableprotocol.ReconfigurableProtocol;

/**
 * Exception that is thrown when it tries to register a already register protocol
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class AlreadyRegisterProtocolException extends Exception {

   public AlreadyRegisterProtocolException(ReconfigurableProtocol protocol) {
      super("The protocol " + protocol + " is already register");
   }
}
