package org.infinispan.counter.impl.atomic;

import org.infinispan.commons.logging.Log;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterState;
import org.infinispan.counter.exception.CounterOutOfBoundsException;
import org.infinispan.counter.impl.listener.CounterEventImpl;
import org.infinispan.counter.impl.listener.CounterManagerNotificationManager;
import org.infinispan.util.logging.LogFactory;

/**
 * //TODO document this!
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class BoundedAtomicStrongCounter extends UnboundedAtomicStrongCounter {

   private static final Log log = LogFactory.getLog(BoundedAtomicStrongCounter.class, Log.class);

   public BoundedAtomicStrongCounter(String name, CounterConfiguration configuration,
         CounterManagerNotificationManager notificationManager) {
      super(name, configuration, notificationManager, BoundedCounterHolder::new);
   }

   private static void throwUpperBoundReached() {
      throw log.counterOurOfBounds(CounterOutOfBoundsException.UPPER_BOUND);
   }

   private static void throwLowerBoundReached() {
      throw log.counterOurOfBounds(CounterOutOfBoundsException.LOWER_BOUND);
   }

   private static void checkUpperBound(CounterState state) {
      if (state == CounterState.UPPER_BOUND_REACHED) {
         throwUpperBoundReached();
      }
   }

   private static void checkLowerBound(CounterState state) {
      if (state == CounterState.LOWER_BOUND_REACHED) {
         throwLowerBoundReached();
      }
   }

   @Override
   long add(CounterHolder data, long delta) {
      if (delta > 0) {
         return addAndCheckUpperBound(data, delta);
      } else {
         return addAndCheckLowerBound(data, delta);
      }
   }

   @Override
   long cas(CounterHolder data, long expect, long update) {
      long retVal = data.getValue();
      if (expect == retVal) {
         if (update < getConfiguration().lowerBound()) {
            throwLowerBoundReached();
         } else if (update > getConfiguration().upperBound()) {
            throwUpperBoundReached();
         }
         data.setValue(update).setState(CounterState.VALID);
      }
      return retVal;

   }

   private long addAndCheckUpperBound(CounterHolder data, long delta) {
      CounterState oldState = data.getState();
      checkUpperBound(oldState);
      long oldValue = data.getValue();
      long upperBound = getConfiguration().upperBound();
      try {
         long addedValue = Math.addExact(oldValue, delta);
         if (addedValue > upperBound) {
            data.setValue(upperBound).setState(CounterState.UPPER_BOUND_REACHED);
         } else {
            data.setValue(addedValue).setState(CounterState.VALID);
         }
      } catch (ArithmeticException e) {
         //overflow!
         data.setValue(Long.MAX_VALUE).setState(CounterState.UPPER_BOUND_REACHED);
      }
      CounterState newState = data.getState();
      triggerEvent(CounterEventImpl.create(oldValue, oldState, data.getValue(), newState));
      checkUpperBound(newState);
      return data.getValue();
   }

   private long addAndCheckLowerBound(CounterHolder data, long delta) {
      CounterState oldState = data.getState();
      checkLowerBound(oldState);
      long oldValue = data.getValue();
      long lowerBound = getConfiguration().lowerBound();
      try {
         long addedValue = Math.addExact(oldValue, delta);
         if (addedValue < lowerBound) {
            data.setValue(lowerBound).setState(CounterState.LOWER_BOUND_REACHED);
         } else {
            data.setValue(addedValue).setState(CounterState.VALID);
         }
      } catch (ArithmeticException e) {
         //overflow!
         data.setValue(Long.MIN_VALUE).setState(CounterState.LOWER_BOUND_REACHED);
      }
      CounterState newState = data.getState();
      triggerEvent(CounterEventImpl.create(oldValue, oldState, data.getValue(), newState));
      checkLowerBound(newState);
      return data.getValue();
   }


}
