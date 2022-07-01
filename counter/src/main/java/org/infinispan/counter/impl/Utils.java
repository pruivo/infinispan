package org.infinispan.counter.impl;

import static org.infinispan.counter.impl.entries.CounterValue.newCounterValue;
import static org.infinispan.counter.logging.Log.CONTAINER;

import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterState;
import org.infinispan.counter.api.CounterType;
import org.infinispan.counter.api.Storage;
import org.infinispan.counter.exception.CounterConfigurationException;
import org.infinispan.counter.impl.entries.CounterValue;
import org.infinispan.functional.Param;

/**
 * Utility methods.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public final class Utils {
   private Utils() {
   }

   /**
    * Validates the lower and upper bound for a strong counter.
    * <p>
    * It throws a {@link CounterConfigurationException} is not valid.
    *
    * @param lowerBound   The counter's lower bound value.
    * @param initialValue The counter's initial value.
    * @param upperBound   The counter's upper bound value.
    * @throws CounterConfigurationException if the upper or lower bound aren't valid.
    */
   public static void validateStrongCounterBounds(long lowerBound, long initialValue, long upperBound) {
      if (lowerBound > initialValue || initialValue > upperBound) {
         throw CONTAINER.invalidInitialValueForBoundedCounter(lowerBound, upperBound, initialValue);
      } else if (lowerBound == upperBound) {
         throw CONTAINER.invalidSameLowerAndUpperBound(lowerBound, upperBound);
      }
   }

   /**
    * Calculates the {@link CounterState} to use based on the value and the boundaries.
    * <p>
    * If the value is less than the lower bound, {@link CounterState#LOWER_BOUND_REACHED} is returned. On other hand, if
    * the value is higher than the upper bound, {@link CounterState#UPPER_BOUND_REACHED} is returned. Otherwise, {@link
    * CounterState#VALID} is returned.
    *
    * @param value      the value to check.
    * @param lowerBound the lower bound.
    * @param upperBound the upper bound.
    * @return the {@link CounterState}.
    */
   public static CounterState calculateState(long value, long lowerBound, long upperBound) {
      if (value < lowerBound) {
         return CounterState.LOWER_BOUND_REACHED;
      } else if (value > upperBound) {
         return CounterState.UPPER_BOUND_REACHED;
      }
      return CounterState.VALID;
   }

   public static Param.PersistenceMode getPersistenceMode(Storage storage) {
      switch (storage) {
         case PERSISTENT:
            return Param.PersistenceMode.LOAD_PERSIST;
         case VOLATILE:
            return Param.PersistenceMode.SKIP;
         default:
            throw new IllegalStateException("[should never happen] unknown storage " + storage);
      }
   }

   public static CounterValue add(CounterConfiguration configuration, CounterValue currentValue, long delta) {
      if (delta == 0) {
         return currentValue;
      }
      if (configuration.type() == CounterType.BOUNDED_STRONG) {
         if (delta > 0) {
            return addAndCheckUpperBound(currentValue, delta, configuration.upperBound());
         } else {
            return addAndCheckLowerBound(currentValue, delta, configuration.lowerBound());
         }
      } else {
         return addUnbounded(currentValue, delta);
      }
   }

   private static CounterValue addAndCheckUpperBound(CounterValue currentValue, long delta, long upperBound) {
      if (currentValue.getState() == CounterState.UPPER_BOUND_REACHED) {
         return currentValue;
      }
      try {
         long addedValue = Math.addExact(currentValue.getValue(), delta);
         if (addedValue > upperBound) {
            return newCounterValue(upperBound, CounterState.UPPER_BOUND_REACHED);
         } else {
            return newCounterValue(addedValue, CounterState.VALID);
         }
      } catch (ArithmeticException e) {
         //overflow!
         return newCounterValue(Long.MAX_VALUE, CounterState.UPPER_BOUND_REACHED);
      }
   }

   private static CounterValue addAndCheckLowerBound(CounterValue currentValue, long delta, long lowerBound) {
      if (currentValue.getState() == CounterState.LOWER_BOUND_REACHED) {
         return currentValue;
      }
      try {
         long addedValue = Math.addExact(currentValue.getValue(), delta);
         if (addedValue < lowerBound) {
            return newCounterValue(lowerBound, CounterState.LOWER_BOUND_REACHED);
         } else {
            return newCounterValue(addedValue, CounterState.VALID);
         }
      } catch (ArithmeticException e) {
         //overflow!
         return newCounterValue(Long.MIN_VALUE, CounterState.LOWER_BOUND_REACHED);
      }
   }

   private static CounterValue addUnbounded(CounterValue currentValue, long delta) {
      if (noChangeRequired(currentValue, delta)) {
         return currentValue;
      }
      try {
         return newCounterValue(Math.addExact(currentValue.getValue(), delta));
      } catch (ArithmeticException e) {
         //overflow!
         return newCounterValue(delta > 0 ? Long.MAX_VALUE : Long.MIN_VALUE);
      }
   }

   private static boolean noChangeRequired(CounterValue value, long delta) {
      return delta > 0 ? value.getValue() == Long.MAX_VALUE : value.getValue() == Long.MIN_VALUE;
   }

   public static CompareAndSwapResult compareAndSwap(CounterConfiguration configuration, CounterValue value, long expected, long update) {
      long retVal = value.getValue();
      if (expected == retVal) {
         if (configuration.type() == CounterType.BOUNDED_STRONG) {
            if (update < configuration.lowerBound()) {
               return CompareAndSwapResult.FAILED_LOWER_BOUND_REACHED;
            } else if (update > configuration.upperBound()) {
               return CompareAndSwapResult.FAILED_UPPER_BOUND_REACHED;
            }
         }
         return CompareAndSwapResult.OK;
      }
      return CompareAndSwapResult.FAILED_WRONG_EXPECTED;
   }

   public enum CompareAndSwapResult {
      FAILED_UPPER_BOUND_REACHED,
      FAILED_LOWER_BOUND_REACHED,
      FAILED_WRONG_EXPECTED,
      OK
   }

   public static void writeLong(long value, byte[] buffer, int offset) {
      if (buffer.length - offset < 8) {
         throw new IllegalArgumentException();
      }
      buffer[offset] = (byte) (value >> 56L);
      buffer[offset+1] = (byte) (value >> 48L);
      buffer[offset+2] = (byte) (value >> 40L);
      buffer[offset+3] = (byte) (value >> 32L);
      buffer[offset+4] = (byte) (value >> 24L);
      buffer[offset+5] = (byte) (value >> 16L);
      buffer[offset+6] = (byte) (value >> 8L);
      buffer[offset+7] = (byte) value;
   }

   public static long readLong(byte[] buffer, int offset) {
      if (buffer.length - offset < 8) {
         throw new IllegalArgumentException();
      }
      return (((long)buffer[offset] << 56) +
            ((long)(buffer[offset + 1] & 0xff) << 48) +
            ((long)(buffer[offset + 2] & 0xff) << 40) +
            ((long)(buffer[offset+ 3] & 0xff) << 32) +
            ((long)(buffer[offset + 4] & 0xff) << 24) +
            ((buffer[offset + 5] & 0xff) << 16) +
            ((buffer[offset + 6] & 0xff) <<  8) +
            (buffer[offset + 7] & 0xff));
   }
}
