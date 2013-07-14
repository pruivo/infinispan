/*
 * INESC-ID, Instituto de Engenharia de Sistemas e Computadores Investigação e Desevolvimento em Lisboa
 * Copyright 2013 INESC-ID and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 3.0 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.stats;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.stats.translations.ExposedStatistics.IspnStats;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


/**
 * @author Diego Didona <didona@gsd.inesc-id.pt>
 * @author Pedro Ruivo
 * @since 5.2
 */
public abstract class TransactionStatistics implements InfinispanStat {

   protected final Log log = LogFactory.getLog(getClass());

   //Here the elements which are common for local and remote transactions
   protected long initTime;
   protected long initCpuTime;
   protected long endLocalTime;
   protected long endLocalCpuTime;
   protected static boolean sampleServiceTime;
   private boolean isReadOnly;
   private boolean isCommit;
   private String transactionalClass;
   private Map<Object, Long> takenLocks = new HashMap<Object, Long>();
   private long lastOpTimestamp;
   private long performedReads;
   protected long readsBeforeFirstWrite = -1;
   private boolean prepareSent = false;

   private final StatisticsContainer statisticsContainer;

   protected Configuration configuration;
   protected static ThreadMXBean threadMXBean;

   protected String id;

   public static void init(Configuration configuration) {
      sampleServiceTime = configuration.customStatsConfiguration().isSampleServiceTimes();
      if (sampleServiceTime)
         threadMXBean = ManagementFactory.getThreadMXBean();
   }

   public Map<Object, Long> getTakenLocks() {
      return takenLocks;
   }

   public void injectId(GlobalTransaction id) {
      this.id = id.globalId();
   }

   public void setReadsBeforeFirstWrite() {
      //I do not use isReadOnly just in case, in the future, we'll be able to tag as update a xact upon start
      if (readsBeforeFirstWrite == -1)
         this.readsBeforeFirstWrite = performedReads;
   }

   public long getReadsBeforeFirstWrite() {
      return readsBeforeFirstWrite;
   }

   public boolean isPrepareSent() {
      return prepareSent;
   }

   public void markPrepareSent() {
      this.prepareSent = true;
   }

   public void incrementNumGets() {
      performedReads++;
   }

   public TransactionStatistics(int size, Configuration configuration) {
      this.initTime = System.nanoTime();
      this.isReadOnly = true; //as far as it does not tries to perform a put operation
      this.takenLocks = new HashMap<Object, Long>();
      this.transactionalClass = TransactionsStatisticsRegistry.DEFAULT_ISPN_CLASS;
      this.statisticsContainer = new StatisticsContainerImpl(size);
      this.configuration = configuration;

      if (log.isTraceEnabled()) {
         log.tracef("Created transaction statistics. Class is %s. Start time is %s",
                    transactionalClass, initTime);
      }
      if (sampleServiceTime) {
         if (log.isTraceEnabled()) log.tracef("Transaction statistics is sampling cpuTime");
         this.initCpuTime = threadMXBean.getCurrentThreadCpuTime();
      }
   }


   public static long getCurrentCpuTime() {
      return threadMXBean.getCurrentThreadCpuTime();
   }

   public final String getTransactionalClass() {
      return this.transactionalClass;
   }

   public final void setTransactionalClass(String className) {
      this.transactionalClass = className;
   }

   public final boolean isCommit() {
      return this.isCommit;
   }

   public final void setCommit(boolean commit) {
      isCommit = commit;
   }

   public final boolean isReadOnly() {
      return this.isReadOnly;
   }

   public final void setUpdateTransaction() {
      this.isReadOnly = false;
   }

   /*TODO: in 2PL it does not really matter *which* are the taken locks if, in the end, we are going to take and average holdTime So we could just use an array of long, without storing the lock itself
     TODO: I could need the actual lock just to be sure it has not already been locked, but I can easily use a hash function for that maybe
  */
   public final void addTakenLock(Object lock) {
      if (!this.takenLocks.containsKey(lock)) {
         long now = System.nanoTime();
         this.takenLocks.put(lock, now);
         if (log.isTraceEnabled())
            log.trace("TID " + Thread.currentThread().getId() + " Added " + lock + " at " + now);
      }
   }

   public final void addValue(IspnStats param, double value) {
      try {
         int index = this.getIndex(param);
         this.statisticsContainer.addValue(index, value);
         if (log.isTraceEnabled()) {
            log.tracef("Add %s to %s", value, param);
         }
      } catch (NoIspnStatException e) {
         log.warnf(e, "Exception caught when trying to add the value %s to %s.", value, param);
      }
   }

   public final long getValue(IspnStats param) {
      int index = this.getIndex(param);
      long value = this.statisticsContainer.getValue(index);
      if (log.isTraceEnabled()) {
         log.tracef("Value of %s is %s", param, value);
      }
      return value;
   }

   public final void incrementValue(IspnStats param) {
      this.addValue(param, 1);
   }

   /**
    * Finalizes statistics of the transaction. This is either called at prepare(1PC)/commit/rollbackCommand visit time
    * for local transactions or upon invocation from InboundInvocationHandler for remote xact
    * (commit/rollback/txCompletionNotification). The remote xact stats container is first "attached" to the thread
    * running the remote transaction and then terminateTransaction is invoked to sample statistics.
    */
   public final void terminateTransaction() {
      if (log.isTraceEnabled()) {
         log.tracef("Terminating transaction. Is read only? %s. Is commit? %s", isReadOnly, isCommit);
      }

       /*
         In case of aborts, locks are *always* released after receiving a RollbackCommand from the coordinator (even if the acquisition fails during the prepare phase)
         This is good, since end of transaction and release of the locks coincide.
         In case of commit we have two cases:
            In some cases we have that the end of xact and release of the locks coincide
            In another case we have that the end of the xact and the release of the locks don't coincide, and the lock holding time has to be "injected" upon release
            this is the case of GMU with commit async, for example, or if locks are actually released upon receiving the TxCompletionNotificationCommand
         */
      int heldLocks = this.takenLocks.size();
      if (heldLocks > 0) {
         boolean remote = !(this instanceof LocalTransactionStatistics);
         if (!LockRelatedStatsHelper.shouldAppendLocks(configuration, isCommit, remote)) {
            if (log.isTraceEnabled())
               log.trace("TID " + Thread.currentThread().getId() + "Sampling locks for " + (remote ? "remote " : "local ") + " transaction " + this.id + " commit? " + isCommit);
            immediateLockingTimeSampling(heldLocks, isCommit);
         } else {
            if (log.isTraceEnabled())
               log.trace("NOT sampling locks for " + (remote ? "remote " : "local ") + " transaction " + this.id);
         }
      }

      double execTime = System.nanoTime() - this.initTime;
      if (this.isReadOnly) {
         if (isCommit) {
            this.incrementValue(IspnStats.NUM_COMMITTED_RO_TX);
            this.addValue(IspnStats.RO_TX_SUCCESSFUL_EXECUTION_TIME, execTime);
            this.addValue(IspnStats.NUM_SUCCESSFUL_GETS_RO_TX, this.getValue(IspnStats.NUM_GET));
            this.addValue(IspnStats.NUM_SUCCESSFUL_REMOTE_GETS_RO_TX, this.getValue(IspnStats.NUM_LOCAL_REMOTE_GET));
         } else {
            this.incrementValue(IspnStats.NUM_ABORTED_RO_TX);
            this.addValue(IspnStats.RO_TX_ABORTED_EXECUTION_TIME, execTime);
         }
      } else {
         if (isCommit) {
            this.incrementValue(IspnStats.NUM_COMMITTED_WR_TX);
            this.addValue(IspnStats.WR_TX_SUCCESSFUL_EXECUTION_TIME, execTime);
            this.addValue(IspnStats.NUM_SUCCESSFUL_GETS_WR_TX, this.getValue(IspnStats.NUM_GET));
            this.addValue(IspnStats.NUM_SUCCESSFUL_REMOTE_GETS_WR_TX, this.getValue(IspnStats.NUM_LOCAL_REMOTE_GET));
            this.addValue(IspnStats.NUM_SUCCESSFUL_PUTS_WR_TX, this.getValue(IspnStats.NUM_PUT));
            this.addValue(IspnStats.NUM_SUCCESSFUL_REMOTE_PUTS_WR_TX, this.getValue(IspnStats.NUM_REMOTE_PUT));

         } else {
            this.incrementValue(IspnStats.NUM_ABORTED_WR_TX);
            this.addValue(IspnStats.WR_TX_ABORTED_EXECUTION_TIME, execTime);
         }
      }

      //TODO: successful lock hold time?

      terminate();
   }

   protected abstract void immediateLockingTimeSampling(int heldLocks, boolean isCommit);


   public final void flush(TransactionStatistics ts) {
      if (log.isTraceEnabled()) {
         log.tracef("Flush this [%s] to %s", this, ts);
      }
      this.statisticsContainer.mergeTo(ts.statisticsContainer);
   }

   public final void dump() {
      this.statisticsContainer.dump();
   }

   @Override
   public String toString() {
      return "initTime=" + initTime +
            ", isReadOnly=" + isReadOnly +
            ", isCommit=" + isCommit +
            ", transactionalClass=" + transactionalClass +
            '}';
   }

   protected abstract int getIndex(IspnStats param);

   protected abstract void onPrepareCommand();

   protected abstract void terminate();

   protected long computeCumulativeLockHoldTime(int numLocks, long currentTime) {
      Set<Map.Entry<Object, Long>> keySet = this.takenLocks.entrySet();
      final boolean trace = (log.isTraceEnabled());
      if (trace)
         log.trace("Held locks from param " + numLocks + "numLocks in entryset " + keySet.size());
      long ret = numLocks * currentTime;
      if (trace)
         log.trace("Now is " + currentTime + "total is " + ret);
      for (Map.Entry<Object, Long> e : keySet) {
         ret -= e.getValue();
         if (trace)
            log.trace("TID " + Thread.currentThread().getId() + " " + e.getKey() + " " + e.getValue());
      }
      if (trace)
         log.trace("TID " + Thread.currentThread().getId() + " " + "Avg lock hold time is " + (ret / (long) numLocks) * 1e-3);
      return ret;
   }

   public void setLastOpTimestamp(long lastOpTimestamp) {
      this.lastOpTimestamp = lastOpTimestamp;
   }

   public long getLastOpTimestamp() {
      return lastOpTimestamp;
   }
}

