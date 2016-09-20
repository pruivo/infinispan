package org.infinispan.marshall.core.internal;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.marshall.core.Ids;

public class PrimitiveExternalizers {

   static final int ARRAY_EMPTY                 = 0x28; // zero elements
   static final int ARRAY_SMALL                 = 0x29; // <=0x100 elements
   static final int ARRAY_MEDIUM                = 0x2A; // <=0x10000 elements
   static final int ARRAY_LARGE                 = 0x2B; // <0x80000000 elements

   static final int SMALL                       = 0x100;
   static final int MEDIUM                      = 0x10000;

   public static final class String extends AbstractExternalizer<java.lang.String> {

      // Assumes that whatever encoding passed can handle ObjectOutput/ObjectInput
      // (that might change when adding support for NIO ByteBuffers)
      final Encoding enc;

      public String(Encoding enc) {
         this.enc = enc;
      }

      @Override
      public Integer getId() {
         return Ids.STRING;
      }

      @Override
      public Set<Class<? extends java.lang.String>> getTypeClasses() {
         return Collections.singleton(java.lang.String.class);
      }

      @Override
      public void writeObject(ObjectOutput out, java.lang.String obj) throws IOException {
         // Instead of out.writeUTF() to be able to write smaller String payloads
         enc.encodeString(obj, out);
      }

      @Override
      public java.lang.String readObject(ObjectInput in) throws IOException, ClassNotFoundException {
         return enc.decodeString(in); // Counterpart to Encoding.encodeString()
      }

   }

   public static final class ByteArray extends AbstractExternalizer<byte[]> {

      @Override
      public Integer getId() {
         return Ids.BYTE_ARRAY;
      }

      @Override
      public Set<Class<? extends byte[]>> getTypeClasses() {
         return Collections.singleton(byte[].class);
      }

      @Override
      public void writeObject(ObjectOutput out, byte[] obj) throws IOException {
         final int len = obj.length;
         if (len == 0) {
            out.writeByte(ARRAY_EMPTY);
         } else if (len <= 256) {
            out.writeByte(ARRAY_SMALL);
            out.writeByte(len);
            out.write(obj, 0, len);
         } else if (len <= 65536) {
            out.writeByte(ARRAY_MEDIUM);
            out.writeShort(len);
            out.write(obj, 0, len);
         } else {
            out.writeByte(ARRAY_LARGE);
            out.writeInt(len);
            out.write(obj, 0, len);
         }
      }

      @Override
      public byte[] readObject(ObjectInput in) throws IOException, ClassNotFoundException {
         byte type = in.readByte();
         switch (type) {
            case ARRAY_EMPTY:
               return new byte[]{};
            case ARRAY_SMALL:
               return readFully(mkByteArray(in.readUnsignedByte(), SMALL), in);
            case ARRAY_MEDIUM:
               return readFully(mkByteArray(in.readUnsignedShort(), MEDIUM), in);
            case ARRAY_LARGE:
               return readFully(new byte[in.readInt()], in);
            default:
               throw new IOException("Unknown array type: " + Integer.toHexString(type));
         }
      }

      private static byte[] mkByteArray(int len, int limit) {
         return new byte[len == 0 ? limit : len];
      }

      private static byte[] readFully(byte[] arr, ObjectInput in) throws IOException {
         in.readFully(arr);
         return arr;
      }

   }

   public static final class Boolean extends AbstractExternalizer<java.lang.Boolean> {

      @Override
      public Integer getId() {
         return Ids.BOOLEAN;
      }

      @Override
      public Set<Class<? extends java.lang.Boolean>> getTypeClasses() {
         return Collections.singleton(java.lang.Boolean.class);
      }

      @Override
      public void writeObject(ObjectOutput out, java.lang.Boolean obj) throws IOException {
         out.writeBoolean(obj.booleanValue());
      }

      @Override
      public java.lang.Boolean readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return input.readBoolean();
      }

   }

   public static final class Byte extends AbstractExternalizer<java.lang.Byte> {

      @Override
      public Integer getId() {
         return Ids.BYTE;
      }

      @Override
      public Set<Class<? extends java.lang.Byte>> getTypeClasses() {
         return Collections.singleton(java.lang.Byte.class);
      }

      @Override
      public void writeObject(ObjectOutput out, java.lang.Byte obj) throws IOException {
         out.writeByte((int) obj);
      }

      @Override
      public java.lang.Byte readObject(ObjectInput in) throws IOException, ClassNotFoundException {
         return in.readByte();
      }

   }

   public static final class Char extends AbstractExternalizer<Character> {

      @Override
      public Integer getId() {
         return Ids.CHAR;
      }

      @Override
      public Set<Class<? extends Character>> getTypeClasses() {
         return Collections.singleton(Character.class);
      }

      @Override
      public void writeObject(ObjectOutput out, Character obj) throws IOException {
         out.writeChar((int) obj);
      }

      @Override
      public Character readObject(ObjectInput in) throws IOException, ClassNotFoundException {
         return in.readChar();
      }

   }

   public static final class Double extends AbstractExternalizer<java.lang.Double> {

      @Override
      public Integer getId() {
         return Ids.DOUBLE;
      }

      @Override
      public Set<Class<? extends java.lang.Double>> getTypeClasses() {
         return Collections.singleton(java.lang.Double.class);
      }

      @Override
      public void writeObject(ObjectOutput out, java.lang.Double obj) throws IOException {
         out.writeDouble(obj.doubleValue());
      }

      @Override
      public java.lang.Double readObject(ObjectInput in) throws IOException, ClassNotFoundException {
         return in.readDouble();
      }

   }

   public static final class Float extends AbstractExternalizer<java.lang.Float> {

      @Override
      public Integer getId() {
         return Ids.FLOAT;
      }

      @Override
      public Set<Class<? extends java.lang.Float>> getTypeClasses() {
         return Collections.singleton(java.lang.Float.class);
      }

      @Override
      public void writeObject(ObjectOutput out, java.lang.Float obj) throws IOException {
         out.writeFloat(obj.floatValue());
      }

      @Override
      public java.lang.Float readObject(ObjectInput in) throws IOException, ClassNotFoundException {
         return in.readFloat();
      }

   }

   public static final class Int extends AbstractExternalizer<java.lang.Integer> {

      @Override
      public Integer getId() {
         return Ids.INTEGER;
      }

      @Override
      public Set<Class<? extends java.lang.Integer>> getTypeClasses() {
         return Collections.singleton(java.lang.Integer.class);
      }

      @Override
      public void writeObject(ObjectOutput out, java.lang.Integer obj) throws IOException {
         out.writeInt(obj.intValue());
      }

      @Override
      public java.lang.Integer readObject(ObjectInput in) throws IOException, ClassNotFoundException {
         return in.readInt();
      }

   }

   public static final class Long extends AbstractExternalizer<java.lang.Long> {

      @Override
      public Integer getId() {
         return Ids.LONG;
      }

      @Override
      public Set<Class<? extends java.lang.Long>> getTypeClasses() {
         return Collections.singleton(java.lang.Long.class);
      }

      @Override
      public void writeObject(ObjectOutput out, java.lang.Long obj) throws IOException {
         out.writeLong(obj.longValue());
      }

      @Override
      public java.lang.Long readObject(ObjectInput in) throws IOException, ClassNotFoundException {
         return in.readLong();
      }

   }

   public static final class Short extends AbstractExternalizer<java.lang.Short> {

      @Override
      public Integer getId() {
         return Ids.SHORT;
      }

      @Override
      public Set<Class<? extends java.lang.Short>> getTypeClasses() {
         return Collections.singleton(java.lang.Short.class);
      }

      @Override
      public void writeObject(ObjectOutput out, java.lang.Short obj) throws IOException {
         out.writeShort(obj.shortValue());
      }

      @Override
      public java.lang.Short readObject(ObjectInput in) throws IOException, ClassNotFoundException {
         return in.readShort();
      }

   }

   public static final class BooleanArray extends AbstractExternalizer<boolean[]> {

      @Override
      public Integer getId() {
         return Ids.BOOLEAN_ARRAY;
      }

      @Override
      public Set<Class<? extends boolean[]>> getTypeClasses() {
         return Collections.singleton(boolean[].class);
      }

      @Override
      public void writeObject(ObjectOutput out, boolean[] obj) throws IOException {
         final int len = obj.length;
         if (len == 0) {
            out.writeByte(ARRAY_EMPTY);
         } else if (len <= 256) {
            out.writeByte(ARRAY_SMALL);
            out.writeByte(len);
            writeBooleans(obj, out);
         } else if (len <= 65536) {
            out.writeByte(ARRAY_MEDIUM);
            out.writeShort(len);
            writeBooleans(obj, out);
         } else {
            out.writeByte(ARRAY_LARGE);
            out.writeInt(len);
            writeBooleans(obj, out);
         }
      }

      private void writeBooleans(boolean[] obj, ObjectOutput out) throws IOException {
         final int len = obj.length;
         final int bc = len & ~7;
         for (int i = 0; i < bc;) {
            out.write(
                  (obj[i++] ? 1 : 0)
                        | (obj[i++] ? 2 : 0)
                        | (obj[i++] ? 4 : 0)
                        | (obj[i++] ? 8 : 0)
                        | (obj[i++] ? 16 : 0)
                        | (obj[i++] ? 32 : 0)
                        | (obj[i++] ? 64 : 0)
                        | (obj[i++] ? 128 : 0)
            );
         }
         if (bc < len) {
            int o = 0;
            int bit = 1;
            for (int i = bc; i < len; i++) {
               if (obj[i]) o |= bit;
               bit <<= 1;
            }
            out.writeByte(o);
         }
      }

      @Override
      public boolean[] readObject(ObjectInput in) throws IOException, ClassNotFoundException {
         byte type = in.readByte();
         switch (type) {
            case ARRAY_EMPTY:
               return new boolean[]{};
            case ARRAY_SMALL:
               return readBooleans(mkBooleanArray(in.readUnsignedByte(), SMALL), in);
            case ARRAY_MEDIUM:
               return readBooleans(mkBooleanArray(in.readUnsignedShort(), MEDIUM), in);
            case ARRAY_LARGE:
               return readBooleans(new boolean[in.readInt()], in);
            default:
               throw new IOException("Unknown array type: " + Integer.toHexString(type));
         }
      }

      private static boolean[] mkBooleanArray(int len, int limit) {
         return new boolean[len == 0 ? limit : len];
      }

      private static boolean[] readBooleans(boolean[] arr, ObjectInput in) throws IOException {
         final int len = arr.length;
         int v;
         int bc = len & ~7;
         for (int i = 0; i < bc; ) {
            v = in.readByte();
            arr[i++] = (v & 1) != 0;
            arr[i++] = (v & 2) != 0;
            arr[i++] = (v & 4) != 0;
            arr[i++] = (v & 8) != 0;
            arr[i++] = (v & 16) != 0;
            arr[i++] = (v & 32) != 0;
            arr[i++] = (v & 64) != 0;
            arr[i++] = (v & 128) != 0;
         }
         if (bc < len) {
            v = in.readByte();
            switch (len & 7) {
               case 7:
                  arr[bc + 6] = (v & 64) != 0;
               case 6:
                  arr[bc + 5] = (v & 32) != 0;
               case 5:
                  arr[bc + 4] = (v & 16) != 0;
               case 4:
                  arr[bc + 3] = (v & 8) != 0;
               case 3:
                  arr[bc + 2] = (v & 4) != 0;
               case 2:
                  arr[bc + 1] = (v & 2) != 0;
               case 1:
                  arr[bc] = (v & 1) != 0;
            }
         }
         return arr;
      }

   }

   public static final class CharArray extends AbstractExternalizer<char[]> {

      @Override
      public Integer getId() {
         return Ids.CHAR_ARRAY;
      }

      @Override
      public Set<Class<? extends char[]>> getTypeClasses() {
         return Collections.singleton(char[].class);
      }

      @Override
      public void writeObject(ObjectOutput out, char[] obj) throws IOException {
         final int len = obj.length;
         if (len == 0) {
            out.writeByte(ARRAY_EMPTY);
         } else if (len <= 256) {
            out.writeByte(ARRAY_SMALL);
            out.writeByte(len);
            for (char v : obj) out.writeChar(v);
         } else if (len <= 65536) {
            out.writeByte(ARRAY_MEDIUM);
            out.writeShort(len);
            for (char v : obj) out.writeChar(v);
         } else {
            out.writeByte(ARRAY_LARGE);
            out.writeInt(len);
            for (char v : obj) out.writeChar(v);
         }
      }

      @Override
      public char[] readObject(ObjectInput in) throws IOException, ClassNotFoundException {
         byte type = in.readByte();
         switch (type) {
            case ARRAY_EMPTY:
               return new char[]{};
            case ARRAY_SMALL:
               return readChars(mkCharArray(in.readUnsignedByte(), SMALL), in);
            case ARRAY_MEDIUM:
               return readChars(mkCharArray(in.readUnsignedShort(), MEDIUM), in);
            case ARRAY_LARGE:
               return readChars(new char[in.readInt()], in);
            default:
               throw new IOException("Unknown array type: " + Integer.toHexString(type));
         }
      }

      private static char[] mkCharArray(int len, int limit) {
         return new char[len == 0 ? limit : len];
      }

      private static char[] readChars(char[] arr, ObjectInput in) throws IOException {
         final int len = arr.length;
         for (int i = 0; i < len; i ++) arr[i] = in.readChar();
         return arr;
      }
   }

   public static final class DoubleArray extends AbstractExternalizer<double[]> {

      @Override
      public Integer getId() {
         return Ids.DOUBLE_ARRAY;
      }

      @Override
      public Set<Class<? extends double[]>> getTypeClasses() {
         return Collections.singleton(double[].class);
      }

      @Override
      public void writeObject(ObjectOutput out, double[] obj) throws IOException {
         final int len = obj.length;
         if (len == 0) {
            out.writeByte(ARRAY_EMPTY);
         } else if (len <= 256) {
            out.writeByte(ARRAY_SMALL);
            out.writeByte(len);
            for (double v : obj) out.writeDouble(v);
         } else if (len <= 65536) {
            out.writeByte(ARRAY_MEDIUM);
            out.writeShort(len);
            for (double v : obj) out.writeDouble(v);
         } else {
            out.writeByte(ARRAY_LARGE);
            out.writeInt(len);
            for (double v : obj) out.writeDouble(v);
         }
      }

      @Override
      public double[] readObject(ObjectInput in) throws IOException, ClassNotFoundException {
         byte type = in.readByte();
         switch (type) {
            case ARRAY_EMPTY:
               return new double[]{};
            case ARRAY_SMALL:
               return readDoubles(mkDoubleArray(in.readUnsignedByte(), SMALL), in);
            case ARRAY_MEDIUM:
               return readDoubles(mkDoubleArray(in.readUnsignedShort(), MEDIUM), in);
            case ARRAY_LARGE:
               return readDoubles(new double[in.readInt()], in);
            default:
               throw new IOException("Unknown array type: " + Integer.toHexString(type));
         }
      }

      private static double[] mkDoubleArray(int len, int limit) {
         return new double[len == 0 ? limit : len];
      }

      private static double[] readDoubles(double[] arr, ObjectInput in) throws IOException {
         final int len = arr.length;
         for (int i = 0; i < len; i ++) arr[i] = in.readDouble();
         return arr;
      }

   }

   public static final class FloatArray extends AbstractExternalizer<float[]> {

      @Override
      public Integer getId() {
         return Ids.FLOAT_ARRAY;
      }

      @Override
      public Set<Class<? extends float[]>> getTypeClasses() {
         return Collections.singleton(float[].class);
      }

      @Override
      public void writeObject(ObjectOutput out, float[] obj) throws IOException {
         final int len = obj.length;
         if (len == 0) {
            out.writeByte(ARRAY_EMPTY);
         } else if (len <= 256) {
            out.writeByte(ARRAY_SMALL);
            out.writeByte(len);
            for (float v : obj) out.writeFloat(v);
         } else if (len <= 65536) {
            out.writeByte(ARRAY_MEDIUM);
            out.writeShort(len);
            for (float v : obj) out.writeFloat(v);
         } else {
            out.writeByte(ARRAY_LARGE);
            out.writeInt(len);
            for (float v : obj) out.writeFloat(v);
         }
      }

      @Override
      public float[] readObject(ObjectInput in) throws IOException, ClassNotFoundException {
         byte type = in.readByte();
         switch (type) {
            case ARRAY_EMPTY:
               return new float[]{};
            case ARRAY_SMALL:
               return readFloats(mkFloatArray(in.readUnsignedByte(), SMALL), in);
            case ARRAY_MEDIUM:
               return readFloats(mkFloatArray(in.readUnsignedShort(), MEDIUM), in);
            case ARRAY_LARGE:
               return readFloats(new float[in.readInt()], in);
            default:
               throw new IOException("Unknown array type: " + Integer.toHexString(type));
         }
      }

      private static float[] mkFloatArray(int len, int limit) {
         return new float[len == 0 ? limit : len];
      }

      private static float[] readFloats(float[] arr, ObjectInput in) throws IOException {
         final int len = arr.length;
         for (int i = 0; i < len; i ++) arr[i] = in.readFloat();
         return arr;
      }

   }

   public static final class IntArray extends AbstractExternalizer<int[]> {

      @Override
      public Integer getId() {
         return Ids.INT_ARRAY;
      }

      @Override
      public Set<Class<? extends int[]>> getTypeClasses() {
         return Collections.singleton(int[].class);
      }

      @Override
      public void writeObject(ObjectOutput out, int[] obj) throws IOException {
         final int len = obj.length;
         if (len == 0) {
            out.writeByte(ARRAY_EMPTY);
         } else if (len <= 256) {
            out.writeByte(ARRAY_SMALL);
            out.writeByte(len);
            for (int v : obj) out.writeInt(v);
         } else if (len <= 65536) {
            out.writeByte(ARRAY_MEDIUM);
            out.writeShort(len);
            for (int v : obj) out.writeInt(v);
         } else {
            out.writeByte(ARRAY_LARGE);
            out.writeInt(len);
            for (int v : obj) out.writeInt(v);
         }
      }

      @Override
      public int[] readObject(ObjectInput in) throws IOException, ClassNotFoundException {
         byte type = in.readByte();
         switch (type) {
            case ARRAY_EMPTY:
               return new int[]{};
            case ARRAY_SMALL:
               return readInts(mkIntArray(in.readUnsignedByte(), SMALL), in);
            case ARRAY_MEDIUM:
               return readInts(mkIntArray(in.readUnsignedShort(), MEDIUM), in);
            case ARRAY_LARGE:
               return readInts(new int[in.readInt()], in);
            default:
               throw new IOException("Unknown array type: " + Integer.toHexString(type));
         }
      }

      private static int[] mkIntArray(int len, int limit) {
         return new int[len == 0 ? limit : len];
      }

      private static int[] readInts(int[] arr, ObjectInput in) throws IOException {
         final int len = arr.length;
         for (int i = 0; i < len; i ++) arr[i] = in.readInt();
         return arr;
      }

   }

   public static final class LongArray extends AbstractExternalizer<long[]> {

      @Override
      public Integer getId() {
         return Ids.LONG_ARRAY;
      }

      @Override
      public Set<Class<? extends long[]>> getTypeClasses() {
         return Collections.singleton(long[].class);
      }

      @Override
      public void writeObject(ObjectOutput out, long[] obj) throws IOException {
         final int len = obj.length;
         if (len == 0) {
            out.writeByte(ARRAY_EMPTY);
         } else if (len <= 256) {
            out.writeByte(ARRAY_SMALL);
            out.writeByte(len);
            for (long v : obj) out.writeLong(v);
         } else if (len <= 65536) {
            out.writeByte(ARRAY_MEDIUM);
            out.writeShort(len);
            for (long v : obj) out.writeLong(v);
         } else {
            out.writeByte(ARRAY_LARGE);
            out.writeInt(len);
            for (long v : obj) out.writeLong(v);
         }
      }

      @Override
      public long[] readObject(ObjectInput in) throws IOException, ClassNotFoundException {
         byte type = in.readByte();
         switch (type) {
            case ARRAY_EMPTY:
               return new long[]{};
            case ARRAY_SMALL:
               return readLongs(mkLongArray(in.readUnsignedByte(), SMALL), in);
            case ARRAY_MEDIUM:
               return readLongs(mkLongArray(in.readUnsignedShort(), MEDIUM), in);
            case ARRAY_LARGE:
               return readLongs(new long[in.readInt()], in);
            default:
               throw new IOException("Unknown array type: " + Integer.toHexString(type));
         }
      }

      private static long[] mkLongArray(int len, int limit) {
         return new long[len == 0 ? limit : len];
      }

      private static long[] readLongs(long[] arr, ObjectInput in) throws IOException {
         final int len = arr.length;
         for (int i = 0; i < len; i ++) arr[i] = in.readLong();
         return arr;
      }

   }

   public static final class ShortArray extends AbstractExternalizer<short[]> {

      @Override
      public Integer getId() {
         return Ids.SHORT_ARRAY;
      }

      @Override
      public Set<Class<? extends short[]>> getTypeClasses() {
         return Collections.singleton(short[].class);
      }

      @Override
      public void writeObject(ObjectOutput out, short[] obj) throws IOException {
         final int len = obj.length;
         if (len == 0) {
            out.writeByte(ARRAY_EMPTY);
         } else if (len <= 256) {
            out.writeByte(ARRAY_SMALL);
            out.writeByte(len);
            for (short v : obj) out.writeShort(v);
         } else if (len <= 65536) {
            out.writeByte(ARRAY_MEDIUM);
            out.writeShort(len);
            for (short v : obj) out.writeShort(v);
         } else {
            out.writeByte(ARRAY_LARGE);
            out.writeInt(len);
            for (short v : obj) out.writeShort(v);
         }
      }

      @Override
      public short[] readObject(ObjectInput in) throws IOException, ClassNotFoundException {
         byte type = in.readByte();
         switch (type) {
            case ARRAY_EMPTY:
               return new short[]{};
            case ARRAY_SMALL:
               return readShorts(mkShortArray(in.readUnsignedByte(), SMALL), in);
            case ARRAY_MEDIUM:
               return readShorts(mkShortArray(in.readUnsignedShort(), MEDIUM), in);
            case ARRAY_LARGE:
               return readShorts(new short[in.readInt()], in);
            default:
               throw new IOException("Unknown array type: " + Integer.toHexString(type));
         }
      }

      private static short[] mkShortArray(int len, int limit) {
         return new short[len == 0 ? limit : len];
      }

      private static short[] readShorts(short[] arr, ObjectInput in) throws IOException {
         final int len = arr.length;
         for (int i = 0; i < len; i ++) arr[i] = in.readShort();
         return arr;
      }

   }

   public static final class ObjectArray extends AbstractExternalizer<Object[]> {

      @Override
      public Integer getId() {
         return Ids.OBJECT_ARRAY;
      }

      @Override
      public Set<Class<? extends Object[]>> getTypeClasses() {
         return Collections.singleton(Object[].class);
      }

      @Override
      public void writeObject(ObjectOutput out, Object[] obj) throws IOException {
         final int len = obj.length;
         if (len == 0) {
            out.writeByte(ARRAY_EMPTY);
         } else if (len <= 256) {
            out.writeByte(ARRAY_SMALL);
            out.writeByte(len);
            for (Object v : obj) out.writeObject(v);
         } else if (len <= 65536) {
            out.writeByte(ARRAY_MEDIUM);
            out.writeShort(len);
            for (Object v : obj) out.writeObject(v);
         } else {
            out.writeByte(ARRAY_LARGE);
            out.writeInt(len);
            for (Object v : obj) out.writeObject(v);
         }
      }

      @Override
      public Object[] readObject(ObjectInput in) throws IOException, ClassNotFoundException {
         byte type = in.readByte();
         switch (type) {
            case ARRAY_EMPTY:
               return new Object[]{};
            case ARRAY_SMALL:
               return readObjects(mkObjectArray(in.readUnsignedByte(), SMALL), in);
            case ARRAY_MEDIUM:
               return readObjects(mkObjectArray(in.readUnsignedShort(), MEDIUM), in);
            case ARRAY_LARGE:
               return readObjects(new Object[in.readInt()], in);
            default:
               throw new IOException("Unknown array type: " + Integer.toHexString(type));
         }
      }

      private static Object[] mkObjectArray(int len, int limit) {
         return new Object[len == 0 ? limit : len];
      }

      private static Object[] readObjects(Object[] arr, ObjectInput in) throws IOException, ClassNotFoundException {
         final int len = arr.length;
         for (int i = 0; i < len; i ++) arr[i] = in.readObject();
         return arr;
      }

   }

}
