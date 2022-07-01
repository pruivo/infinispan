package org.infinispan.counter.impl.raft.operation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.counter.impl.Utils;
import org.infinispan.counter.impl.entries.CounterValue;
import org.infinispan.counter.impl.raft.RaftCounter;
import org.infinispan.util.ByteString;

/**
 * TODO!
 */
public class AddAndGetOperation implements RaftCounterOperation<Long> {

   private final ByteString name;
   private final long delta;

   public AddAndGetOperation(ByteString name, long delta) {
      this.name = name;
      this.delta = delta;
   }

   @Override
   public byte getOperationCode() {
      return OperationCode.ADD_AND_GET;
   }

   @Override
   public ByteString getCounterName() {
      return name;
   }

   @Override
   public ByteBuffer execute(RaftCounter counter) {
      CounterValue result = Utils.add(counter.getConfiguration(), counter.get(), delta);
      switch (result.getState()) {
         case VALID:
            return OperationResult.writeResultWithLong(result.getValue());
         case LOWER_BOUND_REACHED:
            return OperationResult.writeResult(OperationResult.LOWER_BOUND_REACHED);
         case UPPER_BOUND_REACHED:
            return OperationResult.writeResult(OperationResult.UPPER_BOUND_REACHED);
         default:
            throw new IllegalStateException();
      }
   }

   @Override
   public Long readResult(ByteBuffer buffer) {
      return OperationResult.readResultMayBeLong(buffer, RaftCounter.log);
   }

   @Override
   public int serializedSize() {
      // counter name + 8 (delta)
      return name.serializedSize() + 8;
   }

   @Override
   public void writeTo(DataOutput output) throws IOException {
      ByteString.writeObject(output, name);
      output.writeLong(delta);
   }

   public static AddAndGetOperation readFrom(DataInput input) throws IOException {
      return new AddAndGetOperation(ByteString.readObject(input), input.readLong());
   }
}
