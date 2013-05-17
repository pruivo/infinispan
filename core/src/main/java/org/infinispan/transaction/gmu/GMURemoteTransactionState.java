package org.infinispan.transaction.gmu;

import java.util.EnumSet;

/**
 * @author Pedro Ruivo
 * @since 5.2
 */
public class GMURemoteTransactionState {

   private final EnumSet<State> state;

   public GMURemoteTransactionState() {
      state = EnumSet.noneOf(State.class);
   }

   public final boolean preparing() throws InterruptedException {
      synchronized (state) {
         if (state.contains(State.PREPARING)) {
            while (!state.contains(State.PREPARED)) {
               state.wait();
            }
            return false;
         }
         state.add(State.PREPARING);
         return true;
      }
   }

   public final void prepared() {
      synchronized (state) {
         state.add(State.PREPARED);
         state.notifyAll();
      }
   }

   @Override
   public String toString() {
      return "GMURemoteTransactionState{" +
            "state=" + state +
            '}';
   }

   private static enum State {
      /**
       * the prepare command was received and started the validation
       */
      PREPARING,
      /**
       * the prepare command was received and finished the validation
       */
      PREPARED,
   }

}
