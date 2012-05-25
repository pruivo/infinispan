package org.infinispan.reconfigurableprotocol.exception;

/**
 * Indicates that it tried to set two components of the same type for the same protocol
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class AlreadyExistingComponentProtocolException extends Exception {

   public AlreadyExistingComponentProtocolException(String s) {
      super(s);
   }

}
