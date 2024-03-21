package org.infinispan.util.concurrent.locks.impl.jfr;

public interface LockEvent {

   LockEvent NO_OP = new LockEvent() {
      @Override
      public void onAcquire() {
         //no-op
      }

      @Override
      public void onRelease() {
         //no-op
      }

      @Override
      public void onTimeout() {
         //no-op
      }

      @Override
      public void onCancel() {
         //no-op
      }
   };

   void onAcquire();

   void onRelease();

   void onTimeout();

   void onCancel();

   static LockEvent createAcquired(int id, String owner) {
      var event = new LockHoldEvent(id, owner);
      if (!event.isEnabled()) {
         return NO_OP;
      }
      event.onAcquire();
      event.begin();
      return event;
   }

   static LockEvent createPending(int id, String owner) {
      var event = new LockHoldEvent(id, owner);
      if (!event.isEnabled()) {
         return NO_OP;
      }
      event.begin();
      return event;
   }

}
