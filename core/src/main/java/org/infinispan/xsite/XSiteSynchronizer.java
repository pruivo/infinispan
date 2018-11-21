package org.infinispan.xsite;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public class XSiteSynchronizer {

   private final String thisSite;
   private State state;

   public XSiteSynchronizer(State state, String thisSite) {
      this.state = state;
      this.thisSite = thisSite;
   }

   public synchronized boolean isSending() {
      return state == State.SENDING;
   }

   public synchronized boolean isReceiving() {
      return state == State.RECEIVING;
   }


   private enum State {
      /**
       * Initial state when an update is sent to the remote site.
       */
      SENDING,
      /**
       * Initial state when an update is received from the remote site.
       */
      RECEIVING,
      /**
       * In case of conflict, this state indicates that the operation was aborted and this synchronized is no longer
       * valid.
       */
      INVALID,
      /**
       * Final state when the update is completed.
       */
      FINISHED
   }

}
