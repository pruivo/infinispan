package org.infinispan.interceptors.totalorder;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class StateTransferException extends Exception {

   public StateTransferException() {
   }

   public StateTransferException(String message) {
      super(message);
   }

   public StateTransferException(String message, Throwable cause) {
      super(message, cause);
   }

   public StateTransferException(Throwable cause) {
      super(cause);
   }
}
