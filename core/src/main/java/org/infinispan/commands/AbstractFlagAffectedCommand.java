package org.infinispan.commands;

import org.infinispan.commons.util.EnumUtil;
import org.infinispan.context.Flag;

/**
 * Base class for those commands that can carry flags.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public abstract class AbstractFlagAffectedCommand implements FlagAffectedCommand {

   private long flags = 0;
   private TraceCommandData traceCommandData;

   @Override
   public long getFlagsBitSet() {
      return flags;
   }

   @Override
   public void setFlagsBitSet(long bitSet) {
      flags = bitSet;
   }

   protected final boolean hasSameFlags(FlagAffectedCommand other) {
      return flags == other.getFlagsBitSet();
   }

   protected final String printFlags() {
      return EnumUtil.prettyPrintBitSet(flags, Flag.class);
   }

   @Override
   public void setTraceCommandData(TraceCommandData data) {
      traceCommandData = data;
   }

   @Override
   public TraceCommandData getTraceCommandData() {
      return traceCommandData;
   }
}
