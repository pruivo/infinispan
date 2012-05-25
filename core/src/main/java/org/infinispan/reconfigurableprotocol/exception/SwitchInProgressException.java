package org.infinispan.reconfigurableprotocol.exception;

/**
 * Indicates that a switch is in progress
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class SwitchInProgressException extends Exception {

   public SwitchInProgressException(String s) {
      super(s);
   }
}
