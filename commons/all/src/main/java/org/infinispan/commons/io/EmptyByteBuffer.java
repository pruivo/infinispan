package org.infinispan.commons.io;

import org.infinispan.commons.util.Util;

/**
 * TODO!
 */
public enum EmptyByteBuffer implements ByteBuffer {
   INSTANCE;

   @Override
   public byte[] getBuf() {
      return Util.EMPTY_BYTE_ARRAY;
   }

   @Override
   public int getOffset() {
      return 0;
   }

   @Override
   public int getLength() {
      return 0;
   }

   @Override
   public ByteBuffer copy() {
      return INSTANCE;
   }
}
