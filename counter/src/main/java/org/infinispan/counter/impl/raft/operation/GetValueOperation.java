package org.infinispan.counter.impl.raft.operation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.counter.impl.raft.RaftCounter;
import org.infinispan.util.ByteString;

/**
 * TODO! document this
 */
public class GetValueOperation implements RaftCounterOperation<Long> {

   private final ByteString name;

   public GetValueOperation(ByteString name) {
      this.name = name;
   }

   public static RaftCounterOperation<?> readFrom(DataInput input) throws IOException {
      return new GetValueOperation(ByteString.readObject(input));
   }


   @Override
   public byte getOperationCode() {
      return OperationCode.GET;
   }

   @Override
   public ByteString getCounterName() {
      return name;
   }

   @Override
   public ByteBuffer execute(RaftCounter counter) {
      return OperationResult.writeResultWithLong(counter.get().getValue());
   }

   @Override
   public Long readResult(ByteBuffer data) {
      return OperationResult.readResultMayBeLong(data, RaftCounter.log);
   }

   @Override
   public int serializedSize() {
      return name.serializedSize();
   }

   @Override
   public void writeTo(DataOutput output) throws IOException {
      ByteString.writeObject(output, name);
   }
}
