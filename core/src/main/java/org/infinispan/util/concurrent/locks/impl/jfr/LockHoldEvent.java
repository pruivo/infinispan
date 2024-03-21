package org.infinispan.util.concurrent.locks.impl.jfr;

import java.time.Instant;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Enabled;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.StackTrace;
import jdk.jfr.Timestamp;

@Category("Infinispan")
@Label("Lock Event")
@Description("Infinispan lock events")
@StackTrace(false)
@Enabled(false)
public class LockHoldEvent extends Event implements LockEvent {

   @Label("ID")
   public final int lockId;
   @Label("Owner")
   public final String owner;
   @Label("Acquired")
   @Timestamp
   public volatile long acquireTimestamp;
   @Label("Lock status")
   public String status;

   LockHoldEvent(int lockId, String owner) {
      this.lockId = lockId;
      this.owner = owner;
   }

   @Override
   public void onAcquire() {
      status = "acquired";
      acquireTimestamp = Instant.now().toEpochMilli();
   }

   @Override
   public void onRelease() {
      if (shouldCommit()) {
         end();
         commit();
      }
   }

   @Override
   public void onTimeout() {
      if (shouldCommit()) {
         status = "timed-out";
         end();
         commit();
      }
   }

   @Override
   public void onCancel() {
      if (shouldCommit()) {
         status = "cancelled";
         end();
         commit();
      }
   }
}
