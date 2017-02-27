package org.infinispan.commands.write;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.infinispan.commands.ReplicableCommand;

/**
 * A command that represents an acknowledge sent by a backup owner to the originator.
 * <p>
 * The acknowledge signals a successful execution of a multi-key command, like {@link PutMapCommand}. It contains the
 * segments ids of the updated keys.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class BackupMultiKeyAckCommand implements ReplicableCommand {

   public static final byte COMMAND_ID = 41;
   private int segment;
   private long id;
   private int topologyId;

   public BackupMultiKeyAckCommand() {
   }

   public BackupMultiKeyAckCommand(long id, int segment, int topologyId) {
      this.id = id;
      this.segment = segment;
      this.topologyId = topologyId;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }

   @Override
   public boolean canBlock() {
      return false;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeLong(id);
      output.writeInt(segment);
      output.writeInt(topologyId);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      id = input.readLong();
      segment = input.readInt();
      topologyId = input.readInt();
   }

   public int getSegment() {
      return segment;
   }

   public long getId() {
      return id;
   }

   public int getTopologyId() {
      return topologyId;
   }

   @Override
   public String toString() {
      return "BackupMultiKeyAckCommand{" +
            "id=" + id +
            ", segment=" + segment +
            ", topologyId=" + topologyId +
            '}';
   }
}
