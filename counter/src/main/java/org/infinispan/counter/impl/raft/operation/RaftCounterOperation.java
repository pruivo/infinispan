package org.infinispan.counter.impl.raft.operation;

import java.io.DataOutput;
import java.io.IOException;
import java.util.function.Function;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.counter.impl.raft.RaftCounter;
import org.infinispan.util.ByteString;

/**
 * TODO!
 */
public interface RaftCounterOperation<T> extends Function<ByteBuffer, T> {

   byte getOperationCode();

   ByteString getCounterName();

   ByteBuffer execute(RaftCounter counter);

   @Override
   default T apply(ByteBuffer buffer) {
      return readResult(buffer);
   }

   T readResult(ByteBuffer data);

   int serializedSize();

   void writeTo(DataOutput output) throws IOException;

}
