package org.infinispan.counter.impl.raft.operation;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.io.LazyByteArrayOutputStream;

/**
 * TODO!
 */
public enum OperationCode {
   INSTANCE;

   public static final byte CREATE_IF_ABSENT = 0;
   public static final byte ADD_AND_GET = 1;
   public static final byte COMPARE_AND_SWAP = 2;
   public static final byte RESET = 3;

   public static ByteBuffer encode(RaftCounterOperation<?> operation) throws IOException {
      // 1 (operation code) + operation arguments
      int size = 1 + operation.serializedSize();
      LazyByteArrayOutputStream lbaos = new LazyByteArrayOutputStream(size);
      DataOutput output = new DataOutputStream(lbaos);
      output.writeByte(operation.getOperationCode());
      operation.writeTo(output);
      return lbaos.toByteBuffer();
   }

   public static RaftCounterOperation<?> decode(ByteBuffer buffer) throws IOException {
      DataInput input = new DataInputStream(buffer.getStream());
      switch (input.readByte()) {
         case CREATE_IF_ABSENT:
            return CreateCounterOperation.readFrom(input);
         case ADD_AND_GET:
            return AddAndGetOperation.readFrom(input);
         case COMPARE_AND_SWAP:
            return CompareAndSwapOperation.readFrom(input);
         case RESET:
            return ResetOperation.readFrom(input);
         default:
            throw new IllegalStateException();
      }
   }

}
