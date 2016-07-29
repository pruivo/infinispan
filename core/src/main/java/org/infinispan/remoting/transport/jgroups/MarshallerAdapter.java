package org.infinispan.remoting.transport.jgroups;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.jgroups.blocks.Marshaller;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.util.Buffer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.ObjectOutputStream;

/**
 * Bridge between JGroups and Infinispan marshallers
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class MarshallerAdapter implements Marshaller {
   StreamingMarshaller m;

   public MarshallerAdapter(StreamingMarshaller m) {
      this.m = m;
   }

   public Buffer objectToBuffer(Object obj) throws Exception {
      return toBuffer(m.objectToBuffer(obj));
   }

   public Object objectFromBuffer(byte[] buf, int offset, int length) throws Exception {
      return m.objectFromByteBuffer(buf, offset, length);
   }

   private Buffer toBuffer(ByteBuffer bb) {
      return new Buffer(bb.getBuf(), bb.getOffset(), bb.getLength());
   }

   @Override
   public void objectToStream(Object obj, DataOutput out) throws Exception {

   }

   @Override
   public Object objectFromStream(DataInput in) throws Exception {
      return null;
   }
}
