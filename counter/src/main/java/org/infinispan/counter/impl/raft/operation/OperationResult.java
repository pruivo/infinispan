package org.infinispan.counter.impl.raft.operation;

import static org.infinispan.counter.exception.CounterOutOfBoundsException.LOWER_BOUND;
import static org.infinispan.counter.exception.CounterOutOfBoundsException.UPPER_BOUND;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.concurrent.CompletionException;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.io.LazyByteArrayOutputStream;
import org.infinispan.commons.logging.Log;

/**
 * TODO!
 */
public enum OperationResult {
   INSTANCE;


   public static final byte OK = 0;
   public static final byte UPPER_BOUND_REACHED = 1;
   public static final byte LOWER_BOUND_REACHED = 2;

   public static long readResultMayBeLong(ByteBuffer buffer, Log log) {
      DataInputStream dis = new DataInputStream(buffer.getStream());
      try {
         switch (dis.readByte()) {
            case OK:
               return dis.readLong();
            case LOWER_BOUND_REACHED:
               throw new CompletionException(log.counterOurOfBounds(LOWER_BOUND));
            case UPPER_BOUND_REACHED:
               throw new CompletionException(log.counterOurOfBounds(UPPER_BOUND));
            default:
               throw new IllegalStateException();
         }
      } catch (IOException e) {
         throw new IllegalStateException(e);
      }
   }

   public static ByteBuffer writeResultWithLong(long value) {
      // 1 byte + 8 bytes (long)
      LazyByteArrayOutputStream os = new LazyByteArrayOutputStream(9);
      DataOutputStream dos = new DataOutputStream(os);
      try {
         dos.writeByte(OK);
         dos.writeLong(value);
      } catch (IOException e) {
         throw new IllegalStateException(e);
      }
      return os.toByteBuffer();
   }

   public static ByteBuffer writeResult(byte result) {
      return new SingleByteBuffer(result);
   }

   private static class SingleByteBuffer implements ByteBuffer {

      private final byte[] buffer;

      private SingleByteBuffer(byte value) {
         buffer = new byte[]{value};
      }

      @Override
      public byte[] getBuf() {
         return buffer;
      }

      @Override
      public int getOffset() {
         return 0;
      }

      @Override
      public int getLength() {
         return 1;
      }

      @Override
      public ByteBuffer copy() {
         return new SingleByteBuffer(buffer[0]);
      }
   }
}
