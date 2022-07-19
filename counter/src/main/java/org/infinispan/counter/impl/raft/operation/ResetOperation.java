package org.infinispan.counter.impl.raft.operation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.io.EmptyByteBuffer;
import org.infinispan.counter.impl.entries.CounterValue;
import org.infinispan.counter.impl.raft.RaftCounter;
import org.infinispan.util.ByteString;

/**
 * TODO!
 */
public class ResetOperation implements RaftCounterOperation<Void> {

   private final ByteString name;

   public ResetOperation(ByteString name) {
      this.name = name;
   }

   @Override
   public byte getOperationCode() {
      return OperationCode.RESET;
   }

   @Override
   public ByteString getCounterName() {
      return name;
   }

   @Override
   public ByteBuffer execute(RaftCounter counter) {
      counter.set(CounterValue.newCounterValue(counter.getConfiguration()));
      return EmptyByteBuffer.INSTANCE;
   }

   @Override
   public Void readResult(ByteBuffer data) {
      return null;
   }

   @Override
   public int serializedSize() {
      return name.serializedSize();
   }

   @Override
   public void writeTo(DataOutput output) throws IOException {
      ByteString.writeObject(output, name);
   }

   @Override
   public String toString() {
      return "ResetOperation{" +
            "name=" + name +
            '}';
   }

   public static ResetOperation readFrom(DataInput input) throws IOException {
      return new ResetOperation(ByteString.readObject(input));
   }
}
