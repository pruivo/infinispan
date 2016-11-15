package org.infinispan.commands.write;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.DataCommand;

/**
 * Mixes features from DataCommand and WriteCommand
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface DataWriteCommand extends WriteCommand, DataCommand {

   /**
    * @return the {@link CommandInvocationId} associated to the command.
    */
   CommandInvocationId getCommandInvocationId();

   /**
    * Create the {@link BackupWriteCommand} to send to the backup owners.
    * <p>
    * The primary owner is the only member which creates the command to send it.
    *
    * @return the {@link BackupWriteCommand} to send to the backup owners.
    */
   default BackupWriteCommand createBackupWriteCommand() {
      throw new UnsupportedOperationException();
   }

   /**
    * Initializes the primary owner acknowledge with the return value, the {@link CommandInvocationId} and the topology
    * id.
    *  @param command     the {@link PrimaryAckCommand} to initialize.
    * @param localReturnValue the local return value.
    */
   default void initPrimaryAck(PrimaryAckCommand command, Object localReturnValue) {
      throw new UnsupportedOperationException();
   }

}
