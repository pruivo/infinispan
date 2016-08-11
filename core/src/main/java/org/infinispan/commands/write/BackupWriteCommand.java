package org.infinispan.commands.write;

import org.infinispan.atomic.CopyableDeltaAware;
import org.infinispan.atomic.Delta;
import org.infinispan.atomic.DeltaAware;
import org.infinispan.commands.AbstractFlagAffectedCommand;
import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.container.entries.MVCCEntry;
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
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public class BackupWriteCommand extends AbstractFlagAffectedCommand implements VisitableCommand {

   public static final byte COMMAND_ID = 61;
   private static final Log log = LogFactory.getLog(BackupWriteCommand.class);
   private static final boolean trace = log.isTraceEnabled();

   private CommandInvocationId commandInvocationId;
   private Object key;
   private Object value;
   private Metadata metadata;
   private CacheNotifier<Object, Object> notifier;

   public BackupWriteCommand() {
   }

   public BackupWriteCommand(CommandInvocationId commandInvocationId, Object key, Object value, Metadata metadata,
                             CacheNotifier<Object, Object> notifier, long flags) {
      this.commandInvocationId = commandInvocationId;
      this.key = key;
      this.value = value;
      this.metadata = metadata;
      this.notifier = notifier;
      this.setFlagsBitSet(flags);
   }

   private static void unRemoveEntry(MVCCEntry<?, ?> e) {
      e.setCreated(true);
      e.setExpired(false);
      e.setRemoved(false);
      e.setValid(true);
   }

   public void setNotifier(CacheNotifier<Object, Object> notifier) {
      this.notifier = notifier;
   }

   public Object getKey() {
      return key;
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

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      //noinspection unchecked
      MVCCEntry<Object, Object> e = (MVCCEntry<Object, Object>) ctx.lookupEntry(key);
      if (trace) {
         log.tracef("Perform backup operation for key '%s'. Entry='%s'. Is remove? %s",
                    key, e, value == null);
      }
      if (e == null) {
         return null; //non owner
      }
      if (value != null) {
         performPut(e, ctx);
      } else {
         performRemove(e, ctx);
      }
      if (trace) {
         log.tracef("Perform backup operation for key '%s'. Updated Entry='%s'.",
                    key, e);
      }
      return null;
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
      CommandInvocationId.writeTo(output, commandInvocationId);
      output.writeObject(key);
      output.writeObject(value);
      output.writeObject(metadata);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      commandInvocationId = CommandInvocationId.readFrom(input);
      key = input.readObject();
      value = input.readObject();
      metadata = (Metadata) input.readObject();
   }

   @Override
   public String toString() {
      return "BackupWriteCommand{" +
            "key=" + key +
            ", value=" + value +
            ", metadata=" + metadata +
            '}';
   }

   private void performRemove(MVCCEntry<Object, Object> e, InvocationContext ctx) {
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
}
