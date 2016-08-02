package org.infinispan.remoting.transport.jgroups;

import org.infinispan.commons.marshall.NotSerializableException;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jboss.marshalling.ByteInput;
import org.jboss.marshalling.ByteOutput;
import org.jgroups.blocks.Marshaller;
import org.jgroups.util.ByteArrayDataInputStream;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;

/**
 * Bridge between JGroups and Infinispan marshallers
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class MarshallerAdapter implements Marshaller {
   private static final Log log = LogFactory.getLog(MarshallerAdapter.class);
   private static final boolean trace = log.isTraceEnabled();

   StreamingMarshaller m;

   public MarshallerAdapter(StreamingMarshaller m) {
      this.m = m;
   }

   @Override
   public void objectToStream(Object obj, DataOutput out) throws Exception {
      MyByteOutput bo = new MyByteOutput(out);
      ObjectOutput marshaller = m.startObjectOutput(bo, false, 0);
      try {
         m.objectToObjectStream(obj, marshaller);
      } catch (java.io.NotSerializableException nse) {
         if (log.isDebugEnabled())
            log.debug("Object is not serializable", nse);
         throw new NotSerializableException(nse.getMessage(), nse.getCause());
      } catch (IOException ioe) {
         if (ioe.getCause() instanceof InterruptedException) {
            if (trace)
               log.trace("Interrupted exception while marshalling", ioe.getCause());
            throw (InterruptedException) ioe.getCause();
         } else {
            log.errorMarshallingObject(ioe, obj);
            throw ioe;
         }
      } finally {
         m.finishObjectOutput(marshaller);
      }
   }

   @Override
   public Object objectFromStream(DataInput in) throws Exception {
      MyByteInput bi = new MyByteInput(in);
      ObjectInput unmarshaller = m.startObjectInput(bi, false);
      Object o = null;
      try {
         o = m.objectFromObjectStream(unmarshaller);
      } finally {
         m.finishObjectInput(unmarshaller);
      }
      return o;
   }

   @Override
   public int estimatedSize(Object arg) {
      return 0;
   }

   private static class MyByteOutput extends OutputStream implements ByteOutput {
      private final DataOutput out;

      public MyByteOutput(DataOutput out) {
         this.out = out;
      }

      @Override
      public void flush() throws IOException {
         // Do nothing
      }

      @Override
      public void close() throws IOException {
      }

      @Override
      public void write(int b) throws IOException {
         out.write(b);
      }

      @Override
      public void write(byte[] b) throws IOException {
         out.write(b);
      }

      @Override
      public void write(byte[] b, int off, int len) throws IOException {
         out.write(b, off, len);
      }
   }

   private static class MyByteInput extends InputStream implements ByteInput {
      private final ByteArrayDataInputStream in;

      private MyByteInput(DataInput in) {
         this.in = (ByteArrayDataInputStream) in;
      }

      @Override
      public int read() throws IOException {
         return in.readUnsignedByte();
      }

      @Override
      public int read(byte[] b) throws IOException {
         int available = in.limit() - in.position();
         int actual = Math.min(available, b.length);
         in.readFully(b, 0, actual);
         return actual;
      }

      @Override
      public int read(byte[] b, int off, int len) throws IOException {
         int available = in.limit() - in.position();
         int actual = Math.min(available, len);
         in.readFully(b, off, actual);
         return actual;
      }

      @Override
      public int available() throws IOException {
         // Not supported
         return 0;
      }

      @Override
      public long skip(long n) throws IOException {
         return in.skipBytes((int) n);
      }

      @Override
      public void close() throws IOException {
         // Do nothing
      }
   }
}
