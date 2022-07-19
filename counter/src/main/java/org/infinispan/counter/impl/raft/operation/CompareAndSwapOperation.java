package org.infinispan.counter.impl.raft.operation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.invoke.MethodHandles;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.counter.impl.Utils;
import org.infinispan.counter.impl.entries.CounterValue;
import org.infinispan.counter.impl.raft.RaftCounter;
import org.infinispan.counter.logging.Log;
import org.infinispan.util.ByteString;
import org.infinispan.util.logging.LogFactory;

/**
 * TODO!
 */
public class CompareAndSwapOperation implements RaftCounterOperation<Long> {

   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass(), Log.class);

   private final ByteString name;
   private final long expectedValue;
   private final long updatedValue;

   public CompareAndSwapOperation(ByteString name, long expectedValue, long updatedValue) {
      this.name = name;
      this.expectedValue = expectedValue;
      this.updatedValue = updatedValue;
   }

   @Override
   public byte getOperationCode() {
      return OperationCode.COMPARE_AND_SWAP;
   }

   @Override
   public ByteString getCounterName() {
      return name;
   }

   @Override
   public ByteBuffer execute(RaftCounter counter) {
      CounterValue currentValue = counter.get();
      switch (Utils.compareAndSwap(counter.getConfiguration(), currentValue, expectedValue, updatedValue)) {
         case OK:
            counter.set(CounterValue.newCounterValue(updatedValue));
         case FAILED_WRONG_EXPECTED:
            return OperationResult.writeResultWithLong(currentValue.getValue());
         case FAILED_LOWER_BOUND_REACHED:
            return OperationResult.writeResult(OperationResult.LOWER_BOUND_REACHED);
         case FAILED_UPPER_BOUND_REACHED:
            return OperationResult.writeResult(OperationResult.UPPER_BOUND_REACHED);
         default:
            throw new IllegalStateException();
      }
   }

   @Override
   public Long readResult(ByteBuffer data) {
      Long result = OperationResult.readResultMayBeLong(data, RaftCounter.log);
      if (log.isTraceEnabled()) {
         log.tracef("CompareAndSwap %s result is %s", this, result);
      }
      return result;
   }

   @Override
   public int serializedSize() {
      // counter name + 8 (expected value) + 8 (updated value)
      return name.serializedSize() + 16;
   }

   @Override
   public void writeTo(DataOutput output) throws IOException {
      ByteString.writeObject(output, name);
      output.writeLong(expectedValue);
      output.writeLong(updatedValue);
   }

   @Override
   public String toString() {
      return "CompareAndSwapOperation{" +
            "name=" + name +
            ", expectedValue=" + expectedValue +
            ", updatedValue=" + updatedValue +
            '}';
   }

   public static CompareAndSwapOperation readFrom(DataInput input) throws IOException {
      return new CompareAndSwapOperation(ByteString.readObject(input), input.readLong(), input.readLong());
   }
}
