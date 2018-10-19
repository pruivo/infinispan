package org.infinispan.remoting.transport.jgroups.message;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.function.Supplier;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.write.BackupAckCommand;
import org.infinispan.util.ByteString;
import org.jgroups.Address;
import org.jgroups.BaseMessage;
import org.jgroups.Global;
import org.jgroups.Message;
import org.jgroups.util.ByteArray;
import org.jgroups.util.Headers;
import org.jgroups.util.Util;

/**
 * //TODO document this!
 *
 * @author Pedro Ruivo
 * @since 10.0
 */
public class BackupAckMessage extends BaseMessage {

   public static final byte MESSAGE_TYPE = 33;

   private ByteString cacheName;
   private long id;
   private int topologyId;

   public BackupAckMessage(Address dest, BackupAckCommand command) {
      super(dest);
      this.cacheName = command.getCacheName();
      this.id = command.getId();
      this.topologyId = command.getTopologyId();
   }

   private BackupAckMessage(boolean create_headers) {
      super(create_headers);
   }

   public BackupAckMessage() {
   }

   @Override
   public byte getType() {
      return MESSAGE_TYPE;
   }

   @Override
   public Supplier<? extends Message> create() {
      return BackupAckMessage::new;
   }

   @Override
   public BackupAckMessage copy(boolean copy_buffer, boolean copy_headers) {
      BackupAckMessage retval = new BackupAckMessage(false);
      retval.dest_addr = dest_addr;
      retval.src_addr = src_addr;
      short tmp_flags = this.flags;
      byte tmp_tflags = this.transient_flags;
      retval.flags = tmp_flags;
      retval.transient_flags = tmp_tflags;

      if (copy_buffer) {
        retval.cacheName = this.cacheName;
        retval.id = this.id;
        retval.topologyId = this.topologyId;
      }

      //noinspection NonAtomicOperationOnVolatileField
      retval.headers = copy_headers && headers != null ? Headers.copy(this.headers) : createHeaders(
            Util.DEFAULT_HEADERS);
      return retval;
   }

   @Override
   public boolean hasPayload() {
      return true;
   }

   @Override
   public boolean hasArray() {
      return false;
   }

   @Override
   public byte[] getArray() {
      return null;
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
   public <T extends Message> T setArray(byte[] b, int offset, int length) {
      return null;
   }

   @Override
   public <T extends Message> T setArray(ByteArray buf) {
      return null;
   }

   @Override
   public <T> T getObject() {
      return null;
   }

   @Override
   public <T extends Message> T setObject(Object obj) {
      return null;
   }

   @Override
   public int size() {
      return super.size() + cacheName.serializedSize() + Global.INT_SIZE + Global.LONG_SIZE;
   }

   @Override
   public void writeTo(DataOutput out) throws Exception {
      super.writeTo(out);
      writePayload(out);
   }

   @Override
   public void writeToNoAddrs(Address src, DataOutput out, short... excluded_headers) throws Exception {
      super.writeToNoAddrs(src, out, excluded_headers);
      writePayload(out);
   }

   @Override
   public void readFrom(DataInput in) throws Exception {
      super.readFrom(in);
      this.cacheName = ByteString.readObject(in);
      this.id = in.readLong();
      this.topologyId = in.readInt();
   }

   public ReplicableCommand createCommand() {
      return new BackupAckCommand(cacheName, id, topologyId);
   }

   private void writePayload(DataOutput out) throws IOException {
      ByteString.writeObject(out, this.cacheName);
      out.writeLong(id);
      out.writeInt(topologyId);
   }
}
