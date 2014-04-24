package org.infinispan.util.concurrent.locks;

import org.infinispan.util.TimeService;

import java.util.concurrent.TimeUnit;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
public abstract class TimeServiceLockPlaceHolder extends BaseLockPlaceHolder {

   private final TimeService timeService;
   private final long endTime;

   protected TimeServiceLockPlaceHolder(TimeService timeService, long timeout, TimeUnit timeUnit) {
      this.timeService = timeService;
      this.endTime = timeService.expectedEndTime(timeout, timeUnit);
   }

   @Override
   protected final boolean isTimedOut() {
      return timeService.isTimeExpired(endTime);
   }
}
