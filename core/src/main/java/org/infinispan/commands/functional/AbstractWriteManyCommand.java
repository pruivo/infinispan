package org.infinispan.commands.functional;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.write.ValueMatcher;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.functional.impl.Params;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.util.concurrent.locks.RemoteLockCommand;

public abstract class AbstractWriteManyCommand<K, V> implements WriteCommand, FunctionalCommand<K, V>, RemoteLockCommand {

   CommandInvocationId commandInvocationId;
   boolean isForwarded = false;
   int topologyId = -1;
   Params params;
   // TODO: this is used for the non-modifying read-write commands. Move required flags to Params
   // and make sure that ClusteringDependentLogic checks them.
   long flags;
   DataConversion keyDataConversion;
   DataConversion valueDataConversion;
   Map<Object, PrivateMetadata> internalMetadataMap;
   private TraceCommandData traceCommandData;

   protected AbstractWriteManyCommand(CommandInvocationId commandInvocationId,
                                      Params params,
                                      DataConversion keyDataConversion,
                                      DataConversion valueDataConversion) {
      this.commandInvocationId = commandInvocationId;
      this.params = params;
      flags = params.toFlagsBitSet();
      this.keyDataConversion = keyDataConversion;
      this.valueDataConversion = valueDataConversion;
      internalMetadataMap = new ConcurrentHashMap<>();
   }

   protected AbstractWriteManyCommand(AbstractWriteManyCommand<K, V> command) {
      commandInvocationId = command.commandInvocationId;
      topologyId = command.topologyId;
      params = command.params;
      flags = command.flags;
      internalMetadataMap = new ConcurrentHashMap<>(command.internalMetadataMap);
      keyDataConversion = command.keyDataConversion;
      valueDataConversion = command.valueDataConversion;
   }

   protected AbstractWriteManyCommand() {
   }

   @Override
   public void init(ComponentRegistry componentRegistry) {
      componentRegistry.wireDependencies(keyDataConversion);
      componentRegistry.wireDependencies(valueDataConversion);
   }

   @Override
   public int getTopologyId() {
      return topologyId;
   }

   @Override
   public void setTopologyId(int topologyId) {
      this.topologyId = topologyId;
   }

   public boolean isForwarded() {
      return isForwarded;
   }

   public void setForwarded(boolean forwarded) {
      isForwarded = forwarded;
   }

   @Override
   public ValueMatcher getValueMatcher() {
      return ValueMatcher.MATCH_ALWAYS;
   }

   @Override
   public void setValueMatcher(ValueMatcher valueMatcher) {
      // No-op
   }

   @Override
   public boolean isSuccessful() {
      return true;
   }

   @Override
   public boolean isConditional() {
      return false;
   }

   @Override
   public void fail() {
      throw new UnsupportedOperationException();
   }

   @Override
   public long getFlagsBitSet() {
      return flags;
   }

   @Override
   public void setFlagsBitSet(long bitSet) {
      flags = bitSet;
   }

   @Override
   public Params getParams() {
      return params;
   }

   public void setParams(Params params) {
      this.params = params;
   }

   @Override
   public Object getKeyLockOwner() {
      return commandInvocationId;
   }

   @Override
   public CommandInvocationId getCommandInvocationId() {
      return commandInvocationId;
   }

   @Override
   public boolean hasZeroLockAcquisition() {
      return hasAnyFlag(FlagBitSets.ZERO_LOCK_ACQUISITION_TIMEOUT);
   }

   @Override
   public boolean hasSkipLocking() {
      return hasAnyFlag(FlagBitSets.SKIP_LOCKING);
   }

   @Override
   public DataConversion getKeyDataConversion() {
      return keyDataConversion;
   }

   @Override
   public DataConversion getValueDataConversion() {
      return valueDataConversion;
   }

   @Override
   public PrivateMetadata getInternalMetadata(Object key) {
      return internalMetadataMap.get(key);
   }

   @Override
   public void setInternalMetadata(Object key, PrivateMetadata internalMetadata) {
      internalMetadataMap.put(key, internalMetadata);
   }

   @Override
   public TraceCommandData getTraceCommandData() {
      return traceCommandData;
   }

   @Override
   public void setTraceCommandData(TraceCommandData data) {
      traceCommandData = data;
   }
}
