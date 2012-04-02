package org.infinispan.stats.topK;

import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.interceptors.base.BaseCustomInterceptor;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.Parameter;
import org.infinispan.transaction.WriteSkewException;

import java.util.Map;

/**
 * @author Pedro Ruivo
 * @since 5.2
 */
@MBean(objectName = "StreamLibStatistics", description = "Show analytics for workload monitor")
public class StreamLibInterceptor extends BaseCustomInterceptor {

   private static final StreamLibContainer streamLibContainer = StreamLibContainer.getInstance();
   private boolean statisticEnabled = false;
   private DistributionManager distributionManager;

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {

      if (statisticEnabled && ctx.isOriginLocal() && ctx.isInTxScope()) {
         streamLibContainer.addGet(command.getKey(), isRemote(command.getKey()));
      }
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      try {
         if (statisticEnabled && ctx.isOriginLocal() && ctx.isInTxScope()) {
            streamLibContainer.addPut(command.getKey(), isRemote(command.getKey()));
         }
         return invokeNextInterceptor(ctx, command);
      } catch (WriteSkewException wse) {
         Object key = wse.getKey();
         if (key != null && ctx.isOriginLocal()) {
            streamLibContainer.addWriteSkewFailed(key);
         }
         throw wse;
      }
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      try {
         return invokeNextInterceptor(ctx, command);
      } catch (WriteSkewException wse) {
         Object key = wse.getKey();
         if (key != null && ctx.isOriginLocal()) {
            streamLibContainer.addWriteSkewFailed(key);
         }
         throw wse;
      }
   }

   @ManagedOperation(description = "Resets statistics gathered by this component",
                     displayName = "Reset Statistics (Statistics)")
   public void resetStatistics() {
      streamLibContainer.resetAll();
   }

   @ManagedOperation(description = "Set K for the top-K values",
                     displayName = "Set K")
   public void setTopKValue(@Parameter(name = "Top-K", description = "top-Kth to return") int value) {
      streamLibContainer.setCapacity(value);
   }

   @ManagedAttribute(description = "Show the top " + StreamLibContainer.MAX_CAPACITY + " keys most read remotely by this instance",
                     displayName = "Top Remote Read Keys")
   public Map<Object, Long> getRemoteTopGets() {
      return streamLibContainer.getTopKFrom(StreamLibContainer.Stat.REMOTE_GET);
   }

   @ManagedOperation(description = "Show the top n keys most read remotely by this instance",
                     displayName = "N Top Remote Read Keys")
   public Map<Object, Long> getNRemoteTopGets(@Parameter(name = "N th top-key") int n) {
      Map<Object, Long> res = streamLibContainer.getTopKFrom(StreamLibContainer.Stat.REMOTE_GET, n);
      streamLibContainer.resetStat(StreamLibContainer.Stat.REMOTE_GET);
      return res;
   }

   @ManagedAttribute(description = "Show the top " + StreamLibContainer.MAX_CAPACITY + " keys most read locally by this instance",
                     displayName = "Top Local Read Keys")
   public Map<Object, Long> getLocalTopGets() {
      return streamLibContainer.getTopKFrom(StreamLibContainer.Stat.LOCAL_GET);
   }

   @ManagedOperation(description = "Show the top n keys most read locally by this instance",
                     displayName = "N Top Local Read Keys")
   public Map<Object, Long> getNLocalTopGets(@Parameter(name = "N th top-key") int n) {
      Map<Object, Long> res = streamLibContainer.getTopKFrom(StreamLibContainer.Stat.LOCAL_GET, n);
      streamLibContainer.resetStat(StreamLibContainer.Stat.LOCAL_GET);
      return res;
   }

   @ManagedAttribute(description = "Show the top " + StreamLibContainer.MAX_CAPACITY + " keys most write remotely by this instance",
                     displayName = "Top Remote Write Keys")
   public Map<Object, Long> getRemoteTopPuts() {
      return streamLibContainer.getTopKFrom(StreamLibContainer.Stat.REMOTE_PUT);
   }

   @ManagedOperation(description = "Show the top n keys most write remotely by this instance",
                     displayName = "N Top Remote Write Keys")
   public Map<Object, Long> getNRemoteTopPuts(@Parameter(name = "N th top-key") int n) {
      Map<Object, Long> res = streamLibContainer.getTopKFrom(StreamLibContainer.Stat.REMOTE_PUT, n);
      streamLibContainer.resetStat(StreamLibContainer.Stat.REMOTE_PUT);
      return res;
   }

   @ManagedAttribute(description = "Show the top " + StreamLibContainer.MAX_CAPACITY + " keys most write locally by this instance",
                     displayName = "Top Local Write Keys")
   public Map<Object, Long> getLocalTopPuts() {
      return streamLibContainer.getTopKFrom(StreamLibContainer.Stat.LOCAL_PUT);
   }

   @ManagedOperation(description = "Show the top n keys most write locally by this instance",
                     displayName = "N Top Local Write Keys")
   public Map<Object, Long> getNLocalTopPuts(@Parameter(name = "N th top-key") int n) {
      Map<Object, Long> res = streamLibContainer.getTopKFrom(StreamLibContainer.Stat.LOCAL_PUT, n);
      streamLibContainer.resetStat(StreamLibContainer.Stat.LOCAL_PUT);
      return res;
   }

   @ManagedAttribute(description = "Show the top " + StreamLibContainer.MAX_CAPACITY + " keys most locked",
                     displayName = "Top Locked Keys")
   public Map<Object, Long> getTopLockedKeys() {
      return streamLibContainer.getTopKFrom(StreamLibContainer.Stat.MOST_LOCKED_KEYS);
   }

   @ManagedOperation(description = "Show the top n keys most locked",
                     displayName = "N Top Locked Keys")
   public Map<Object, Long> getNTopLockedKeys(@Parameter(name = "N th top-key") int n) {
      Map<Object, Long> res = streamLibContainer.getTopKFrom(StreamLibContainer.Stat.MOST_LOCKED_KEYS, n);
      streamLibContainer.resetStat(StreamLibContainer.Stat.MOST_LOCKED_KEYS);
      return res;
   }

   @ManagedAttribute(description = "Show the top " + StreamLibContainer.MAX_CAPACITY + " keys most contended",
                     displayName = "Top Contended Keys")
   public Map<Object, Long> getTopContendedKeys() {
      return streamLibContainer.getTopKFrom(StreamLibContainer.Stat.MOST_CONTENDED_KEYS);
   }

   @ManagedOperation(description = "Show the top n keys most contended",
                     displayName = "N Top Contended Keys")
   public Map<Object, Long> getNTopContendedKeys(@Parameter(name = "N th top-key") int n) {
      Map<Object, Long> res = streamLibContainer.getTopKFrom(StreamLibContainer.Stat.MOST_CONTENDED_KEYS, n);
      streamLibContainer.resetStat(StreamLibContainer.Stat.MOST_CONTENDED_KEYS);
      return res;
   }

   @ManagedAttribute(description = "Show the top " + StreamLibContainer.MAX_CAPACITY + " keys whose lock acquisition failed by timeout",
                     displayName = "Top Keys whose Lock Acquisition Failed by Timeout")
   public Map<Object, Long> getTopLockFailedKeys() {
      return streamLibContainer.getTopKFrom(StreamLibContainer.Stat.MOST_FAILED_KEYS);
   }

   @ManagedOperation(description = "Show the top n keys whose lock acquisition failed ",
                     displayName = "N Top Keys whose Lock Acquisition Failed by Timeout")
   public Map<Object, Long> getNTopLockFailedKeys(@Parameter(name = "N th top-key") int n) {
      Map<Object, Long> res = streamLibContainer.getTopKFrom(StreamLibContainer.Stat.MOST_FAILED_KEYS, n);
      streamLibContainer.resetStat(StreamLibContainer.Stat.MOST_FAILED_KEYS);
      return res;
   }

   @ManagedAttribute(description = "Show the top " + StreamLibContainer.MAX_CAPACITY + " keys whose write skew check was failed",
                     displayName = "Top Keys whose Write Skew Check was failed")
   public Map<Object, Long> getTopWriteSkewFailedKeys() {
      return streamLibContainer.getTopKFrom(StreamLibContainer.Stat.MOST_WRITE_SKEW_FAILED_KEYS);
   }

   @ManagedOperation(description = "Show the top n keys whose write skew check was failed",
                     displayName = "N Top Keys whose Write Skew Check was failed")
   public Map<Object, Long> getNTopWriteSkewFailedKeys(@Parameter(name = "N th top-key") int n) {
      Map<Object, Long> res = streamLibContainer.getTopKFrom(StreamLibContainer.Stat.MOST_WRITE_SKEW_FAILED_KEYS, n);
      streamLibContainer.resetStat(StreamLibContainer.Stat.MOST_WRITE_SKEW_FAILED_KEYS);
      return res;
   }

   @ManagedOperation(description = "Show the top n keys whose write skew check was failed",
                     displayName = "Top Keys whose Write Skew Check was failed")
   public void setStatisticsEnabled(@Parameter(name = "Enabled?") boolean enabled) {
      statisticEnabled = true;
      streamLibContainer.setActive(enabled);
   }

   @Override
   protected void start() {
      super.start();
      setStatisticsEnabled(true);
      this.distributionManager = cache.getAdvancedCache().getDistributionManager();
   }

   private boolean isRemote(Object k) {
      return distributionManager != null && distributionManager.getLocality(k).isLocal();
   }
}
