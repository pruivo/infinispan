package org.infinispan.commands.write;

import org.infinispan.atomic.CopyableDeltaAware;
import org.infinispan.atomic.Delta;
import org.infinispan.atomic.DeltaAware;
import org.infinispan.commands.AbstractFlagAffectedCommand;
import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.Metadatas;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * A command sent from the primary owner to the backup owners of a key with the new update.
 * <p>
 * This command is only visited by the backups owner and in a remote context. No locks are acquired since it is sent in
 * FIFO order. It can represent a update or remove operation.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class BackupWriteCommand extends AbstractFlagAffectedCommand implements VisitableCommand, TopologyAffectedCommand {

   public static final byte COMMAND_ID = 61;
   private static final Log log = LogFactory.getLog(BackupWriteCommand.class);
   private static final boolean trace = log.isTraceEnabled();
   private static final Operation[] CACHED_VALUES = Operation.values();

   private Operation operation;
   private CommandInvocationId commandInvocationId;
   private Object key;
   private Object value;
   private Metadata metadata;
   private CacheNotifier<Object, Object> notifier;
   private int topologyId;
   //used in query interceptor only.
   private boolean nonExistent;

   public BackupWriteCommand() {
   }

   private BackupWriteCommand(Operation operation, CommandInvocationId commandInvocationId, Object key, Object value,
                              Metadata metadata, long flags, int topologyId) {
      this.operation = operation;
      this.commandInvocationId = commandInvocationId;
      this.key = key;
      this.value = value;
      this.metadata = metadata;
      this.setFlagsBitSet(flags);
      this.topologyId = topologyId;
   }

   private static void unRemoveEntry(MVCCEntry<?, ?> e) {
      e.setCreated(true);
      e.setExpired(false);
      e.setRemoved(false);
      e.setValid(true);
   }

   private static Operation valueOf(int index) {
      return CACHED_VALUES[index];
   }

   public static BackupWriteCommand constructWrite(CommandInvocationId id, Object key, Object value, Metadata metadata, long flags, int topologyId) {
      return new BackupWriteCommand(Operation.WRITE, id, key, value, metadata, flags, topologyId);
   }

   public static BackupWriteCommand constructRemove(CommandInvocationId id, Object key, long flags, int topologyId) {
      return new BackupWriteCommand(Operation.REMOVE, id, key, null, null, flags, topologyId);
   }

   public static BackupWriteCommand constructRemoveExpired(CommandInvocationId id, Object key, long flags, int topologyId) {
      return new BackupWriteCommand(Operation.REMOVE_EXPIRED, id, key, null, null, flags, topologyId);
   }

   public void setNotifier(CacheNotifier<Object, Object> notifier) {
      this.notifier = notifier;
   }

   public Object getKey() {
      return key;
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      //noinspection unchecked
      MVCCEntry<Object, Object> e = (MVCCEntry<Object, Object>) ctx.lookupEntry(key);

      if (trace) {
         log.tracef("Perform backup operation (%s) for key '%s'. Current Entry='%s'.",
                    operation, key, e);
      }
      if (e == null) {
         return null; //non owner
      }

      switch (operation) {
         case WRITE:
            performPut(e, ctx);
            break;
         case REMOVE:
            performRemove(e, ctx);
            break;
         case REMOVE_EXPIRED:
            performExpiration(e);
            break;
      }

      if (trace) {
         log.tracef("Perform backup operation (%s) for key '%s'. Updated Entry='%s'.",
                    operation, key, e);
      }
      return null;
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitBackupWriteCommand(ctx, this);
   }

   @Override
   public boolean shouldInvoke(InvocationContext ctx) {
      return true;
   }

   @Override
   public boolean ignoreCommandOnStatus(ComponentStatus status) {
      return false;
   }

   @Override
   public boolean readsExistingValues() {
      return false;
   }

   public CommandInvocationId getCommandInvocationId() {
      return commandInvocationId;
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
      return true;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      MarshallUtil.marshallEnum(operation, output);
      CommandInvocationId.writeTo(output, commandInvocationId);
      output.writeObject(key);
      switch (operation) {
         case WRITE:
            output.writeObject(value);
            output.writeObject(metadata);
            break;
         default:
      }
      output.writeLong(Flag.copyWithoutRemotableFlags(getFlagsBitSet()));
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      operation = MarshallUtil.unmarshallEnum(input, BackupWriteCommand::valueOf);
      commandInvocationId = CommandInvocationId.readFrom(input);
      key = input.readObject();
      switch (operation) {
         case WRITE:
            value = input.readObject();
            metadata = (Metadata) input.readObject();
            break;
         default:
      }
      setFlagsBitSet(input.readLong());
   }

   public Metadata getMetadata() {
      return metadata;
   }

   @Override
   public String toString() {
      return "BackupWriteCommand{" +
            "operation=" + operation +
            ", commandInvocationId=" + commandInvocationId +
            ", key=" + key +
            ", value=" + value +
            ", metadata=" + metadata +
            ", topologyId=" + topologyId +
            '}';
   }

   public boolean isRemove() {
      return operation == Operation.REMOVE || operation == Operation.REMOVE_EXPIRED;
   }

   @Override
   public int getTopologyId() {
      return topologyId;
   }

   @Override
   public void setTopologyId(int topologyId) {
      this.topologyId = topologyId;
   }

   public Object getValue() {
      return value;
   }

   public boolean isNonExistent() {
      return nonExistent;
   }

   private void performExpiration(MVCCEntry<?, ?> e) {
      nonExistent = e.isNull();
      if (!e.isRemoved()) {
         e.setValue(null);
         e.setRemoved(true);
         e.setChanged(true);
         e.setValid(false);
         e.setExpired(true);
      }
   }

   private void performRemove(MVCCEntry<?, ?> e, InvocationContext ctx) {
      nonExistent = e.isNull();
      notifier.notifyCacheEntryRemoved(key, e.getValue(), e.getMetadata(), true, ctx, this);
      e.setValue(null);
      e.setRemoved(true);
      e.setChanged(true);
      e.setValid(false);
   }

   private void performPut(MVCCEntry<Object, Object> e, InvocationContext ctx) {
      Object entryValue = e.getValue();

      if (e.isCreated()) {
         notifier.notifyCacheEntryCreated(key, value, metadata, true, ctx, this);
      } else {
         notifier.notifyCacheEntryModified(key, value, metadata, entryValue, e.getMetadata(), true, ctx, this);
      }

      if (value instanceof Delta) {
         // magic
         Delta dv = (Delta) value;
         if (e.isRemoved()) {
            unRemoveEntry(e);
            e.setValue(dv.merge(null));
            Metadatas.updateMetadata(e, metadata);
         } else {
            DeltaAware toMergeWith = null;
            if (entryValue instanceof CopyableDeltaAware) {
               toMergeWith = ((CopyableDeltaAware) entryValue).copy();
            } else if (entryValue instanceof DeltaAware) {
               toMergeWith = (DeltaAware) entryValue;
            }
            e.setValue(dv.merge(toMergeWith));
            Metadatas.updateMetadata(e, metadata);
         }
      } else {
         e.setValue(value);
         Metadatas.updateMetadata(e, metadata);
         if (e.isRemoved()) {
            unRemoveEntry(e);
         }
      }
      e.setChanged(true);
   }

   private enum Operation {
      WRITE,
      REMOVE,
      REMOVE_EXPIRED
   }
}
