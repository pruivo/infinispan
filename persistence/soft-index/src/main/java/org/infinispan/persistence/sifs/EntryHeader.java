package org.infinispan.persistence.sifs;

import java.nio.ByteBuffer;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class EntryHeader {
   static final int MAGIC = 0xBE11A61C;
   static final boolean useMagic = false;
   static final int HEADER_SIZE = 28 + (useMagic ? 4 : 0);

   private final int keyLength;
   private final int valueLength;
   private final int metadataLength;
   private final long seqId;
   private final long expiration;
   private final int internalMetadataLength;

   public EntryHeader(ByteBuffer buffer) {
      if (useMagic) {
         if (buffer.getInt() != MAGIC) throw new IllegalStateException();
      }
      this.keyLength = buffer.getShort();
      this.metadataLength = buffer.getShort();
      this.valueLength = buffer.getInt();
      this.internalMetadataLength = buffer.getInt();
      this.seqId = buffer.getLong();
      this.expiration = buffer.getLong();
   }

   public int keyLength() {
      return keyLength;
   }

   public int metadataLength() {
      return metadataLength;
   }

   public int internalMetadataLength() {
      return internalMetadataLength;
   }

   public int valueLength() {
      return valueLength;
   }

   public long seqId() {
      return seqId;
   }

   public long expiryTime() {
      return expiration;
   }

   @Override
   public String toString() {
      return String.format("[keyLength=%d, valueLength=%d, metadataLength=%d, internalMetadataLength=%d,seqId=%d, expiration=%d]", keyLength, valueLength, metadataLength, internalMetadataLength, seqId, expiration);
   }

   public int totalLength() {
      return keyLength + metadataLength + internalMetadataLength + valueLength + HEADER_SIZE;
   }
}
