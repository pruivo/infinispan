package org.infinispan.client.hotrod.impl.transaction.entry;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public enum  ControlByte {
   NOT_READ(0x1),
   NON_EXISTING(0x2),
   REMOVE_OP(0x4);

   private final byte bit;

   ControlByte(int bit) {
      this.bit = (byte) bit;
   }

   public static String prettyPrint(byte bitSet) {
      StringBuilder builder = new StringBuilder("[");
      if (NOT_READ.hasFlag(bitSet)) {
         builder.append("NOT_READ");
      } else if (NON_EXISTING.hasFlag(bitSet)) {
         builder.append("NON_EXISTING");
      } else {
         builder.append("READ");
      }
      if (REMOVE_OP.hasFlag(bitSet)) {
         builder.append(", REMOVED");
      }
      return builder.append("]").toString();
   }

   /**
    * Sets {@code this} flag to the {@code bitSet}.
    *
    * @return The new bit set.
    */
   public byte set(byte bitSet) {
      return (byte) (bitSet | bit);
   }

   /**
    * @return {@code true} if {@code this} flag is set in the {@code bitSet}, {@code false} otherwise.
    */
   public boolean hasFlag(byte bitSet) {
      return (bitSet & bit) == bit;
   }

   /**
    * @return The bit corresponding to {@code this} flag.
    */
   public byte bit() {
      return bit;
   }
}
