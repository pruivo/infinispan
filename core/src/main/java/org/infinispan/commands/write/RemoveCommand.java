package org.infinispan.commands.write;

import static org.infinispan.commons.util.Util.toStr;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.Visitor;
import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;


/**
 * @author Mircea.Markus@jboss.com
 * @author <a href="mailto:galder.zamarreno@jboss.com">Galder Zamarreno</a>
 * @since 4.0
 */
public class RemoveCommand extends AbstractDataWriteCommand {
   private static final Log log = LogFactory.getLog(RemoveCommand.class);
   public static final byte COMMAND_ID = 10;
   protected CacheNotifier notifier;
   boolean successful = true;
   boolean nonExistent = false;

   protected ValueMatcher valueMatcher;
   protected Equivalence valueEquivalence;

   /**
    * When not null, value indicates that the entry should only be removed if the key is mapped to this value.
    * When null, the entry should be removed regardless of what value it is mapped to.
    */
   protected Object value;

   public RemoveCommand(Object key, Object value, CacheNotifier notifier, long flagsBitSet, Equivalence valueEquivalence, CommandInvocationId commandInvocationId) {
      super(key, flagsBitSet, commandInvocationId);
      this.value = value;
      this.notifier = notifier;
      this.valueEquivalence = valueEquivalence;
      this.valueMatcher = value != null ? ValueMatcher.MATCH_EXPECTED : ValueMatcher.MATCH_ALWAYS;
   }

   public void init(CacheNotifier notifier, Configuration configuration) {
      this.notifier = notifier;
      this.valueEquivalence = configuration.dataContainer().valueEquivalence();
   }

   public RemoveCommand() {
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitRemoveCommand(ctx, this);
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      // It's not worth looking up the entry if we're never going to apply the change.
      if (valueMatcher == ValueMatcher.MATCH_NEVER) {
         successful = false;
         return null;
      }
      MVCCEntry e = (MVCCEntry) ctx.lookupEntry(key);
      if (e == null || e.isNull() || e.isRemoved()) {
         nonExistent = true;
         if (valueMatcher.matches(e, value, null, valueEquivalence)) {
            if (e != null) {
               e.setChanged(true);
               e.setRemoved(true);
            }
            return isConditional() ? true : null;
         } else {
            log.trace("Nothing to remove since the entry doesn't exist in the context or it is null");
            successful = false;
            return false;
         }
      }

      if (!valueMatcher.matches(e, value, null, valueEquivalence)) {
         successful = false;
         return false;
      }

      if (this instanceof EvictCommand) {
         e.setEvicted(true);
      }

      return performRemove(e, ctx);
   }

   public void notify(InvocationContext ctx, Object removedValue, Metadata removedMetadata, boolean isPre) {
      notifier.notifyCacheEntryRemoved(key, removedValue, removedMetadata, isPre, ctx, this);
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public boolean equals(Object o) {
      if (!super.equals(o)) {
         return false;
      }

   RemoveCommand that = (RemoveCommand) o;

      if (value != null ? !value.equals(that.value) : that.value != null) {
         return false;
      }

      return true;
   }

   @Override
   public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (value != null ? value.hashCode() : 0);
      return result;
   }


   @Override
   public String toString() {
      return new StringBuilder()
         .append("RemoveCommand{key=")
         .append(toStr(key))
         .append(", value=").append(toStr(value))
         .append(", flags=").append(printFlags())
         .append(", valueMatcher=").append(valueMatcher)
         .append("}")
         .toString();
   }

   @Override
   public boolean isSuccessful() {
      return successful;
   }

   @Override
   public boolean isConditional() {
      return value != null;
   }

   public boolean isNonExistent() {
      return nonExistent;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeObject(key);
      output.writeObject(value);
      output.writeLong(Flag.copyWithoutRemotableFlags(getFlagsBitSet()));
      MarshallUtil.marshallEnum(valueMatcher, output);
      CommandInvocationId.writeTo(output, commandInvocationId);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      key = input.readObject();
      value = input.readObject();
      setFlagsBitSet(input.readLong());
      valueMatcher = MarshallUtil.unmarshallEnum(input, ValueMatcher::valueOf);
      commandInvocationId = CommandInvocationId.readFrom(input);
   }

   @Override
   public ValueMatcher getValueMatcher() {
      return valueMatcher;
   }

   @Override
   public void setValueMatcher(ValueMatcher valueMatcher) {
      this.valueMatcher = valueMatcher;
   }

   @Override
   public void updateStatusFromRemoteResponse(Object remoteResponse) {
      // Remove without an expected value can't fail
      if (value != null) {
         successful = (Boolean) remoteResponse;
      }
   }

   @Override
   public boolean ignoreCommandOnStatus(ComponentStatus status) {
      return false;
   }

   @Override
   public boolean readsExistingValues() {
      return value != null || !hasFlag(Flag.IGNORE_RETURN_VALUES);
   }

   public Object getValue() {
      return value;
   }

   public void setValue(Object value) {
      this.value = value;
   }

   @Override
   public final boolean isReturnValueExpected() {
      // IGNORE_RETURN_VALUES ignored for conditional remove
      return isConditional() || super.isReturnValueExpected();
   }

   @Override
   public BackupWriteCommand createBackupWriteCommand() {
      return new BackupWriteCommand(commandInvocationId, key, null, null, notifier, getFlagsBitSet());
   }

   @Override
   public void initPrimaryAck(PrimaryAckCommand command, Object returnValue) {
      if (isConditional()) {
         command.initWithBoolReturnValue(successful, successful);
      } else if (hasFlag(Flag.IGNORE_RETURN_VALUES)) {
         command.initWithoutReturnValue(successful);
      } else {
         command.initWithReturnValue(successful, returnValue);
      }
   }

   protected Object performRemove(CacheEntry e, InvocationContext ctx) {
      final Object removedValue = e.getValue();
      notify(ctx, removedValue, e.getMetadata(), true);

      e.setRemoved(true);
      e.setValid(false);
      e.setChanged(true);

      if (valueMatcher != ValueMatcher.MATCH_EXPECTED_OR_NEW) {
         return isConditional() ? true : removedValue;
      } else {
         // Return the expected value when retrying
         return isConditional() ? true : value;
      }
   }
}
