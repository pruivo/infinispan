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
import org.infinispan.stats.percentiles.PercentileStats;
import org.infinispan.stats.percentiles.PercentileStatsFactory;
import org.infinispan.stats.translations.ExposedStatistics.IspnStats;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import static org.infinispan.stats.translations.ExposedStatistics.IspnStats.*;


/**
 * Websiste: www.cloudtm.eu Date: 01/05/12
 *
 * @author Diego Didona <didona@gsd.inesc-id.pt>
 * @author Pedro Ruivo
 * @since 5.2
 */
public class NodeScopeStatisticCollector {
   private final static Log log = LogFactory.getLog(NodeScopeStatisticCollector.class);

   private LocalTransactionStatistics localTransactionStatistics;
   private RemoteTransactionStatistics remoteTransactionStatistics;

   private PercentileStats localTransactionWrExecutionTime;
   private PercentileStats remoteTransactionWrExecutionTime;
   private PercentileStats localTransactionRoExecutionTime;
   private PercentileStats remoteTransactionRoExecutionTime;

   private Configuration configuration;


   private long lastResetTime;

   public final synchronized void reset() {
      if (log.isTraceEnabled()) {
         log.tracef("Resetting Node Scope Statistics");
      }
      this.localTransactionStatistics = new LocalTransactionStatistics(this.configuration);
      this.remoteTransactionStatistics = new RemoteTransactionStatistics(this.configuration);

      this.localTransactionRoExecutionTime = PercentileStatsFactory.createNewPercentileStats();
      this.localTransactionWrExecutionTime = PercentileStatsFactory.createNewPercentileStats();
      this.remoteTransactionRoExecutionTime = PercentileStatsFactory.createNewPercentileStats();
      this.remoteTransactionWrExecutionTime = PercentileStatsFactory.createNewPercentileStats();

      this.lastResetTime = System.nanoTime();
   }

   public NodeScopeStatisticCollector(Configuration configuration) {
      this.configuration = configuration;
      reset();
   }

   public final synchronized void merge(TransactionStatistics ts) {
      if (log.isTraceEnabled()) {
         log.tracef("Merge transaction statistics %s to the node statistics", ts);
      }
      if (ts instanceof LocalTransactionStatistics) {
         ts.flush(this.localTransactionStatistics);
         if (ts.isCommit()) {
            if (ts.isReadOnly()) {
               this.localTransactionRoExecutionTime.insertSample(ts.getValue(IspnStats.RO_TX_SUCCESSFUL_EXECUTION_TIME));
            } else {
               this.localTransactionWrExecutionTime.insertSample(ts.getValue(IspnStats.WR_TX_SUCCESSFUL_EXECUTION_TIME));
            }
         } else {
            if (ts.isReadOnly()) {
               this.localTransactionRoExecutionTime.insertSample(ts.getValue(IspnStats.RO_TX_ABORTED_EXECUTION_TIME));
            } else {
               this.localTransactionWrExecutionTime.insertSample(ts.getValue(IspnStats.WR_TX_ABORTED_EXECUTION_TIME));
            }
         }
      } else if (ts instanceof RemoteTransactionStatistics) {
         ts.flush(this.remoteTransactionStatistics);
         if (ts.isCommit()) {
            if (ts.isReadOnly()) {
               this.remoteTransactionRoExecutionTime.insertSample(ts.getValue(IspnStats.RO_TX_SUCCESSFUL_EXECUTION_TIME));
            } else {
               this.remoteTransactionWrExecutionTime.insertSample(ts.getValue(IspnStats.WR_TX_SUCCESSFUL_EXECUTION_TIME));
            }
         } else {
            if (ts.isReadOnly()) {
               this.remoteTransactionRoExecutionTime.insertSample(ts.getValue(IspnStats.RO_TX_ABORTED_EXECUTION_TIME));
            } else {
               this.remoteTransactionWrExecutionTime.insertSample(ts.getValue(IspnStats.WR_TX_ABORTED_EXECUTION_TIME));
            }
         }
      }
   }

   public final synchronized void addLocalValue(IspnStats stat, double value) {
      localTransactionStatistics.addValue(stat, value);
   }

   public final synchronized void addRemoteValue(IspnStats stat, double value) {
      remoteTransactionStatistics.addValue(stat, value);
   }


   public final synchronized double getPercentile(IspnStats param, int percentile) throws NoIspnStatException {
      if (log.isTraceEnabled()) {
         log.tracef("Get percentile %s from %s", percentile, param);
      }
      switch (param) {
         case RO_LOCAL_PERCENTILE:
            return localTransactionRoExecutionTime.getKPercentile(percentile);
         case WR_LOCAL_PERCENTILE:
            return localTransactionWrExecutionTime.getKPercentile(percentile);
         case RO_REMOTE_PERCENTILE:
            return remoteTransactionRoExecutionTime.getKPercentile(percentile);
         case WR_REMOTE_PERCENTILE:
            return remoteTransactionWrExecutionTime.getKPercentile(percentile);
         default:
            throw new NoIspnStatException("Invalid percentile " + param);
      }
   }

   /*
   Can I invoke this synchronized method from inside itself??
    */

   @SuppressWarnings("UnnecessaryBoxing")
   public final synchronized Object getAttribute(IspnStats param) throws NoIspnStatException {
      if (log.isTraceEnabled()) {
         log.tracef("Get attribute %s", param);
      }
      switch (param) {
         case NUM_EARLY_ABORTS: {
            return localTransactionStatistics.getValue(param);
         }
         case NUM_LOCALPREPARE_ABORTS: {
            return localTransactionStatistics.getValue(param);
         }
         case NUM_REMOTELY_ABORTED: {
            return localTransactionStatistics.getValue(param);
         }
         case NUM_UPDATE_TX_GOT_TO_PREPARE: {
            return localTransactionStatistics.getValue(param);
         }
         case NUM_WAITS_IN_COMMIT_QUEUE: {
            return localTransactionStatistics.getValue(param);
         }
         case NUM_WAITS_IN_REMOTE_COMMIT_QUEUE: {
            return remoteTransactionStatistics.getValue(NUM_WAITS_IN_COMMIT_QUEUE);
         }
         case NUM_WAITS_REMOTE_REMOTE_GETS: {
            return remoteTransactionStatistics.getValue(param);
         }
         case LOCK_HOLD_TIME: {
            long localLocks = localTransactionStatistics.getValue(IspnStats.NUM_HELD_LOCKS);
            long remoteLocks = remoteTransactionStatistics.getValue(IspnStats.NUM_HELD_LOCKS);
            if ((localLocks + remoteLocks) != 0) {
               long localHoldTime = localTransactionStatistics.getValue(IspnStats.LOCK_HOLD_TIME);
               long remoteHoldTime = remoteTransactionStatistics.getValue(IspnStats.LOCK_HOLD_TIME);
               return new Long(convertNanosToMicro(localHoldTime + remoteHoldTime) / (localLocks + remoteLocks));
            }
            return new Long(0);
         }
         case SUX_LOCK_HOLD_TIME: {
            return microAvgLocal(NUM_SUX_LOCKS, SUX_LOCK_HOLD_TIME);
         }
         case LOCAL_ABORT_LOCK_HOLD_TIME: {
            return microAvgLocal(NUM_LOCAL_ABORTED_LOCKS, LOCAL_ABORT_LOCK_HOLD_TIME);
         }
         case REMOTE_ABORT_LOCK_HOLD_TIME: {
            return microAvgLocal(NUM_REMOTE_ABORTED_LOCKS, REMOTE_ABORT_LOCK_HOLD_TIME);
         }
         case LOCAL_REMOTE_GET_S:
            return microAvgLocal(IspnStats.NUM_LOCAL_REMOTE_GET, IspnStats.LOCAL_REMOTE_GET_S);
         case LOCAL_REMOTE_GET_R:
            return microAvgLocal(IspnStats.NUM_LOCAL_REMOTE_GET, IspnStats.LOCAL_REMOTE_GET_R);
         case RTT_PREPARE:
            return microAvgLocal(IspnStats.NUM_RTTS_PREPARE, IspnStats.RTT_PREPARE);
         case RTT_COMMIT:
            return microAvgLocal(IspnStats.NUM_RTTS_COMMIT, IspnStats.RTT_COMMIT);
         case RTT_ROLLBACK:
            return microAvgLocal(IspnStats.NUM_RTTS_ROLLBACK, IspnStats.RTT_ROLLBACK);
         case RTT_GET:
            return microAvgLocal(IspnStats.NUM_RTTS_GET, IspnStats.RTT_GET);
         case ASYNC_COMMIT:
            return microAvgLocal(IspnStats.NUM_ASYNC_COMMIT, IspnStats.ASYNC_COMMIT);
         case ASYNC_COMPLETE_NOTIFY:
            return microAvgLocal(IspnStats.NUM_ASYNC_COMPLETE_NOTIFY, IspnStats.ASYNC_COMPLETE_NOTIFY);
         case ASYNC_PREPARE:
            return microAvgLocal(IspnStats.NUM_ASYNC_PREPARE, IspnStats.ASYNC_PREPARE);
         case ASYNC_ROLLBACK:
            return microAvgLocal(IspnStats.NUM_ASYNC_ROLLBACK, IspnStats.ASYNC_ROLLBACK);
         case NUM_NODES_COMMIT:
            return avgMultipleLocalCounters(IspnStats.NUM_NODES_COMMIT, IspnStats.NUM_RTTS_COMMIT, IspnStats.NUM_ASYNC_COMMIT);
         case NUM_NODES_GET:
            return avgMultipleLocalCounters(IspnStats.NUM_NODES_GET, IspnStats.NUM_RTTS_GET);
         case NUM_NODES_PREPARE:
            return avgMultipleLocalCounters(IspnStats.NUM_NODES_PREPARE, IspnStats.NUM_RTTS_PREPARE, IspnStats.NUM_ASYNC_PREPARE);
         case NUM_NODES_ROLLBACK:
            return avgMultipleLocalCounters(IspnStats.NUM_NODES_ROLLBACK, IspnStats.NUM_RTTS_ROLLBACK, IspnStats.NUM_ASYNC_ROLLBACK);
         case NUM_NODES_COMPLETE_NOTIFY:
            return avgMultipleLocalCounters(IspnStats.NUM_NODES_COMPLETE_NOTIFY, IspnStats.NUM_ASYNC_COMPLETE_NOTIFY);
         case PUTS_PER_LOCAL_TX: {
            return avgDoubleLocal(IspnStats.NUM_COMMITTED_WR_TX, IspnStats.NUM_SUCCESSFUL_PUTS);
         }
         case LOCAL_CONTENTION_PROBABILITY: {
            long numLocalPuts = localTransactionStatistics.getValue(IspnStats.NUM_PUT);
            if (numLocalPuts != 0) {
               long numLocalLocalContention = localTransactionStatistics.getValue(IspnStats.LOCK_CONTENTION_TO_LOCAL);
               long numLocalRemoteContention = localTransactionStatistics.getValue(IspnStats.LOCK_CONTENTION_TO_REMOTE);
               return new Double((numLocalLocalContention + numLocalRemoteContention) * 1.0 / numLocalPuts);
            }
            return new Double(0);
         }
         case REMOTE_CONTENTION_PROBABILITY: {
            long numRemotePuts = remoteTransactionStatistics.getValue(IspnStats.NUM_PUT);
            if (numRemotePuts != 0) {
               long numRemoteLocalContention = remoteTransactionStatistics.getValue(IspnStats.LOCK_CONTENTION_TO_LOCAL);
               long numRemoteRemoteContention = remoteTransactionStatistics.getValue(IspnStats.LOCK_CONTENTION_TO_REMOTE);
               return new Double((numRemoteLocalContention + numRemoteRemoteContention) * 1.0 / numRemotePuts);
            }
            return new Double(0);
         }
         case LOCK_CONTENTION_PROBABILITY: {
            long numLocalPuts = localTransactionStatistics.getValue(IspnStats.NUM_PUT);
            long numRemotePuts = remoteTransactionStatistics.getValue(IspnStats.NUM_PUT);
            long totalPuts = numLocalPuts + numRemotePuts;
            if (totalPuts != 0) {
               long localLocal = localTransactionStatistics.getValue(IspnStats.LOCK_CONTENTION_TO_LOCAL);
               long localRemote = localTransactionStatistics.getValue(IspnStats.LOCK_CONTENTION_TO_REMOTE);
               long remoteLocal = remoteTransactionStatistics.getValue(IspnStats.LOCK_CONTENTION_TO_LOCAL);
               long remoteRemote = remoteTransactionStatistics.getValue(IspnStats.LOCK_CONTENTION_TO_REMOTE);
               long totalCont = localLocal + localRemote + remoteLocal + remoteRemote;
               return new Double(totalCont / totalPuts);
            }
            return new Double(0);
         }

         case LOCK_CONTENTION_TO_LOCAL: {
            return new Long(localTransactionStatistics.getValue(param));
         }
         case LOCK_CONTENTION_TO_REMOTE: {
            return new Long(localTransactionStatistics.getValue(param));
         }
         case REMOTE_LOCK_CONTENTION_TO_LOCAL: {
            return new Long(localTransactionStatistics.getValue(LOCK_CONTENTION_TO_LOCAL));
         }

         case REMOTE_LOCK_CONTENTION_TO_REMOTE: {
            return new Long(localTransactionStatistics.getValue(LOCK_CONTENTION_TO_REMOTE));
         }

         case NUM_OWNED_WR_ITEMS_IN_LOCAL_PREPARE: {
            return avgDoubleLocal(NUM_UPDATE_TX_PREPARED, IspnStats.NUM_OWNED_WR_ITEMS_IN_OK_PREPARE);
         }
         case NUM_OWNED_WR_ITEMS_IN_REMOTE_PREPARE: {
            return avgDoubleRemote(NUM_UPDATE_TX_PREPARED, IspnStats.NUM_OWNED_WR_ITEMS_IN_OK_PREPARE);
         }

         case NUM_OWNED_RD_ITEMS_IN_LOCAL_PREPARE: {
            return avgDoubleLocal(IspnStats.NUM_UPDATE_TX_PREPARED, IspnStats.NUM_OWNED_RD_ITEMS_IN_OK_PREPARE);
         }
         case NUM_OWNED_RD_ITEMS_IN_REMOTE_PREPARE: {
            return avgDoubleRemote(IspnStats.NUM_UPDATE_TX_PREPARED, IspnStats.NUM_OWNED_RD_ITEMS_IN_OK_PREPARE);
         }



        /*
         Local execution times
          */
         case UPDATE_TX_LOCAL_S: {
            return microAvgLocal(IspnStats.NUM_UPDATE_TX_LOCAL_COMMIT, IspnStats.UPDATE_TX_LOCAL_S);
         }
         case UPDATE_TX_LOCAL_R: {
            return microAvgLocal(IspnStats.NUM_UPDATE_TX_LOCAL_COMMIT, IspnStats.UPDATE_TX_LOCAL_R);
         }
         case READ_ONLY_TX_LOCAL_S: {
            return microAvgLocal(IspnStats.NUM_READ_ONLY_TX_COMMIT, IspnStats.READ_ONLY_TX_LOCAL_S);
         }
         case READ_ONLY_TX_LOCAL_R: {
            return microAvgLocal(IspnStats.NUM_READ_ONLY_TX_COMMIT, IspnStats.READ_ONLY_TX_LOCAL_R);
         }
         /*
         Prepare Times (only relevant to successfully executed prepareCommand...even for xact that eventually abort...)
         //TODO: is it the case to take also this stat only for xacts that commit?
          */

         case UPDATE_TX_LOCAL_PREPARE_R: {
            return microAvgLocal(IspnStats.NUM_UPDATE_TX_PREPARED, IspnStats.UPDATE_TX_LOCAL_PREPARE_R);
         }

         case UPDATE_TX_LOCAL_PREPARE_S: {
            return microAvgLocal(IspnStats.NUM_UPDATE_TX_PREPARED, IspnStats.UPDATE_TX_LOCAL_PREPARE_S);
         }
         case UPDATE_TX_REMOTE_PREPARE_R: {
            return microAvgRemote(IspnStats.NUM_UPDATE_TX_PREPARED, IspnStats.UPDATE_TX_REMOTE_PREPARE_R);
         }
         case UPDATE_TX_REMOTE_PREPARE_S: {
            return microAvgRemote(IspnStats.NUM_UPDATE_TX_PREPARED, IspnStats.UPDATE_TX_REMOTE_PREPARE_S);
         }
         case READ_ONLY_TX_PREPARE_R: {
            return microAvgRemote(NUM_READ_ONLY_TX_COMMIT, IspnStats.READ_ONLY_TX_PREPARE_R);
         }
         case READ_ONLY_TX_PREPARE_S: {
            return microAvgRemote(NUM_READ_ONLY_TX_COMMIT, IspnStats.READ_ONLY_TX_PREPARE_S);
         }

          /*
             Commit Times
           */

         case UPDATE_TX_LOCAL_COMMIT_R: {
            return microAvgLocal(IspnStats.NUM_UPDATE_TX_LOCAL_COMMIT, IspnStats.UPDATE_TX_LOCAL_COMMIT_R);
         }
         case UPDATE_TX_LOCAL_COMMIT_S: {
            return microAvgLocal(IspnStats.NUM_UPDATE_TX_LOCAL_COMMIT, IspnStats.UPDATE_TX_LOCAL_COMMIT_S);
         }
         case UPDATE_TX_REMOTE_COMMIT_R: {
            return microAvgRemote(IspnStats.NUM_UPDATE_TX_REMOTE_COMMIT, IspnStats.UPDATE_TX_REMOTE_COMMIT_R);
         }
         case UPDATE_TX_REMOTE_COMMIT_S: {
            return microAvgRemote(IspnStats.NUM_UPDATE_TX_REMOTE_COMMIT, IspnStats.UPDATE_TX_REMOTE_COMMIT_S);
         }
         case READ_ONLY_TX_COMMIT_R: {
            return microAvgRemote(NUM_READ_ONLY_TX_COMMIT, IspnStats.READ_ONLY_TX_COMMIT_R);
         }
         case READ_ONLY_TX_COMMIT_S: {
            return microAvgRemote(NUM_READ_ONLY_TX_COMMIT, IspnStats.READ_ONLY_TX_COMMIT_S);
         }

          /*
          Rollback times
           */

         case UPDATE_TX_REMOTE_ROLLBACK_R: {
            return microAvgRemote(NUM_UPDATE_TX_REMOTE_ROLLBACK, IspnStats.UPDATE_TX_REMOTE_ROLLBACK_R);
         }
         case UPDATE_TX_REMOTE_ROLLBACK_S: {
            return microAvgRemote(NUM_UPDATE_TX_REMOTE_ROLLBACK, IspnStats.UPDATE_TX_REMOTE_ROLLBACK_S);
         }
         case UPDATE_TX_LOCAL_REMOTE_ROLLBACK_R: {
            return microAvgLocal(NUM_UPDATE_TX_LOCAL_REMOTE_ROLLBACK, IspnStats.UPDATE_TX_LOCAL_REMOTE_ROLLBACK_R);
         }
         case UPDATE_TX_LOCAL_REMOTE_ROLLBACK_S: {
            return microAvgLocal(NUM_UPDATE_TX_LOCAL_REMOTE_ROLLBACK, IspnStats.UPDATE_TX_LOCAL_REMOTE_ROLLBACK_S);
         }
         case UPDATE_TX_LOCAL_LOCAL_ROLLBACK_R: {
            return microAvgLocal(NUM_UPDATE_TX_LOCAL_LOCAL_ROLLBACK, IspnStats.UPDATE_TX_LOCAL_LOCAL_ROLLBACK_R);
         }
         case UPDATE_TX_LOCAL_LOCAL_ROLLBACK_S: {
            return microAvgLocal(NUM_UPDATE_TX_LOCAL_LOCAL_ROLLBACK, IspnStats.UPDATE_TX_LOCAL_LOCAL_ROLLBACK_S);
         }
         case REMOTE_REMOTE_GET_WAITING_TIME: {
            return microAvgRemote(NUM_WAITS_REMOTE_REMOTE_GETS, REMOTE_REMOTE_GET_WAITING_TIME);
         }
         case REMOTE_REMOTE_GET_R: {
            return microAvgRemote(NUM_REMOTE_REMOTE_GETS, REMOTE_REMOTE_GET_R);
         }
         case REMOTE_REMOTE_GET_S: {
            return microAvgRemote(NUM_REMOTE_REMOTE_GETS, REMOTE_REMOTE_GET_S);
         }
         case FIRST_WRITE_INDEX: {
            return avgDoubleLocal(IspnStats.NUM_COMMITTED_WR_TX, FIRST_WRITE_INDEX);
         }
         case LOCK_WAITING_TIME: {
            long localWaitedForLocks = localTransactionStatistics.getValue(IspnStats.NUM_WAITED_FOR_LOCKS);
            long remoteWaitedForLocks = remoteTransactionStatistics.getValue(IspnStats.NUM_WAITED_FOR_LOCKS);
            long totalWaitedForLocks = localWaitedForLocks + remoteWaitedForLocks;
            if (totalWaitedForLocks != 0) {
               long localWaitedTime = localTransactionStatistics.getValue(IspnStats.LOCK_WAITING_TIME);
               long remoteWaitedTime = remoteTransactionStatistics.getValue(IspnStats.LOCK_WAITING_TIME);
               return new Long(convertNanosToMicro(localWaitedTime + remoteWaitedTime) / totalWaitedForLocks);
            }
            return new Long(0);
         }
         case TX_WRITE_PERCENTAGE: {     //computed on the locally born txs
            long readTx = localTransactionStatistics.getValue(IspnStats.NUM_READ_ONLY_TX_COMMIT) +
                    localTransactionStatistics.getValue(NUM_ABORTED_RO_TX);
            long writeTx = localTransactionStatistics.getValue(IspnStats.NUM_COMMITTED_WR_TX) +
                    localTransactionStatistics.getValue(NUM_ABORTED_WR_TX);
            long total = readTx + writeTx;
            if (total != 0)
               return new Double(writeTx * 1.0 / total);
            return new Double(0);
         }
         case SUCCESSFUL_WRITE_PERCENTAGE: { //computed on the locally born txs
            long readSuxTx = localTransactionStatistics.getValue(IspnStats.NUM_READ_ONLY_TX_COMMIT);
            long writeSuxTx = localTransactionStatistics.getValue(IspnStats.NUM_COMMITTED_WR_TX);
            long total = readSuxTx + writeSuxTx;
            if (total != 0) {
               return new Double(writeSuxTx * 1.0 / total);
            }
            return new Double(0);
         }
         case APPLICATION_CONTENTION_FACTOR: {
            long localTakenLocks = localTransactionStatistics.getValue(IspnStats.NUM_HELD_LOCKS);
            long remoteTakenLocks = remoteTransactionStatistics.getValue(IspnStats.NUM_HELD_LOCKS);
            long elapsedTime = System.nanoTime() - this.lastResetTime;
            double totalLocksArrivalRate = (localTakenLocks + remoteTakenLocks) / convertNanosToMicro(elapsedTime);
            long holdTime = (Long) this.getAttribute(IspnStats.LOCK_HOLD_TIME);

            if ((totalLocksArrivalRate * holdTime) != 0) {
               double lockContProb = (Double) this.getAttribute(IspnStats.LOCK_CONTENTION_PROBABILITY);
               return new Double(lockContProb / (totalLocksArrivalRate * holdTime));
            }
            return new Double(0);
         }
         case NUM_SUCCESSFUL_GETS_RO_TX:
            return avgLocal(IspnStats.NUM_READ_ONLY_TX_COMMIT, NUM_SUCCESSFUL_GETS_RO_TX);
         case NUM_SUCCESSFUL_GETS_WR_TX:
            return avgLocal(IspnStats.NUM_COMMITTED_WR_TX, NUM_SUCCESSFUL_GETS_WR_TX);
         case NUM_SUCCESSFUL_REMOTE_GETS_RO_TX:
            return avgDoubleLocal(IspnStats.NUM_READ_ONLY_TX_COMMIT, IspnStats.NUM_SUCCESSFUL_REMOTE_GETS_RO_TX);
         case NUM_SUCCESSFUL_REMOTE_GETS_WR_TX:
            return avgDoubleLocal(IspnStats.NUM_COMMITTED_WR_TX, IspnStats.NUM_SUCCESSFUL_REMOTE_GETS_WR_TX);

         case NUM_SUCCESSFUL_PUTS_WR_TX:
            return avgDoubleLocal(IspnStats.NUM_COMMITTED_WR_TX, IspnStats.NUM_SUCCESSFUL_PUTS_WR_TX);
         case NUM_SUCCESSFUL_REMOTE_PUTS_WR_TX:
            return avgDoubleLocal(IspnStats.NUM_COMMITTED_WR_TX, IspnStats.NUM_SUCCESSFUL_REMOTE_PUTS_WR_TX);
         case REMOTE_PUT_EXECUTION:
            return microAvgLocal(IspnStats.NUM_REMOTE_PUT, IspnStats.REMOTE_PUT_EXECUTION);
         case NUM_LOCK_FAILED_DEADLOCK:
         case NUM_LOCK_FAILED_TIMEOUT:
            return new Long(localTransactionStatistics.getValue(param));
         case NUM_READLOCK_FAILED_TIMEOUT:
            return new Long(localTransactionStatistics.getValue(param));
         case WR_TX_SUCCESSFUL_EXECUTION_TIME:
            return microAvgLocal(IspnStats.NUM_COMMITTED_WR_TX, IspnStats.WR_TX_SUCCESSFUL_EXECUTION_TIME);
         case WR_TX_ABORTED_EXECUTION_TIME:
            return microAvgLocal(IspnStats.NUM_ABORTED_WR_TX, IspnStats.WR_TX_ABORTED_EXECUTION_TIME);
         case PREPARE_COMMAND_SIZE:
            return avgMultipleLocalCounters(IspnStats.PREPARE_COMMAND_SIZE, IspnStats.NUM_RTTS_PREPARE, IspnStats.NUM_ASYNC_PREPARE);
         case ROLLBACK_COMMAND_SIZE:
            return avgMultipleLocalCounters(IspnStats.ROLLBACK_COMMAND_SIZE, IspnStats.NUM_RTTS_ROLLBACK, IspnStats.NUM_ASYNC_ROLLBACK);
         case COMMIT_COMMAND_SIZE:
            return avgMultipleLocalCounters(IspnStats.COMMIT_COMMAND_SIZE, IspnStats.NUM_RTTS_COMMIT, IspnStats.NUM_ASYNC_COMMIT);
         case CLUSTERED_GET_COMMAND_SIZE:
            return avgLocal(IspnStats.NUM_RTTS_GET, IspnStats.CLUSTERED_GET_COMMAND_SIZE);
         case REMOTE_REMOTE_GET_REPLY_SIZE:
            return avgRemote(IspnStats.NUM_REMOTE_REMOTE_GETS, REMOTE_REMOTE_GET_REPLY_SIZE);
         case NUM_LOCK_PER_LOCAL_TX:
            return avgMultipleLocalCounters(IspnStats.NUM_HELD_LOCKS, IspnStats.NUM_COMMITTED_WR_TX, NUM_ABORTED_WR_TX);
         case NUM_LOCK_PER_REMOTE_TX:
            return avgMultipleRemoteCounters(IspnStats.NUM_HELD_LOCKS, IspnStats.NUM_COMMITTED_WR_TX, NUM_ABORTED_WR_TX);
         case NUM_LOCK_PER_SUCCESS_LOCAL_TX:
            return avgLocal(IspnStats.NUM_COMMITTED_WR_TX, IspnStats.NUM_HELD_LOCKS_SUCCESS_TX);
         case TX_COMPLETE_NOTIFY_EXECUTION_TIME:
            return microAvgRemote(IspnStats.NUM_TX_COMPLETE_NOTIFY_COMMAND, IspnStats.TX_COMPLETE_NOTIFY_EXECUTION_TIME);
         case UPDATE_TX_TOTAL_R: {
            return microAvgLocal(IspnStats.NUM_COMMITTED_WR_TX, UPDATE_TX_TOTAL_R);
         }
         case UPDATE_TX_TOTAL_S: {
            return microAvgLocal(IspnStats.NUM_COMMITTED_WR_TX, UPDATE_TX_TOTAL_S);
         }
         case READ_ONLY_TX_TOTAL_R: {
            return microAvgLocal(NUM_READ_ONLY_TX_COMMIT, READ_ONLY_TX_TOTAL_R);
         }
         case READ_ONLY_TX_TOTAL_S: {
            return microAvgLocal(NUM_READ_ONLY_TX_COMMIT, READ_ONLY_TX_TOTAL_S);
         }
         case ABORT_RATE:
            long totalAbort = localTransactionStatistics.getValue(NUM_ABORTED_RO_TX) +
                    localTransactionStatistics.getValue(NUM_ABORTED_WR_TX);
            long totalCommitAndAbort = localTransactionStatistics.getValue(IspnStats.NUM_READ_ONLY_TX_COMMIT) +
                    localTransactionStatistics.getValue(IspnStats.NUM_COMMITTED_WR_TX) + totalAbort;
            if (totalCommitAndAbort != 0) {
               return new Double(totalAbort * 1.0 / totalCommitAndAbort);
            }
            return new Double(0);
         case NUM_ABORTED_WR_TX: {
            return new Long(localTransactionStatistics.getValue(param));
         }
         case ARRIVAL_RATE:
            long localCommittedTx = localTransactionStatistics.getValue(IspnStats.NUM_READ_ONLY_TX_COMMIT) +
                    localTransactionStatistics.getValue(IspnStats.NUM_COMMITTED_WR_TX);
            long localAbortedTx = localTransactionStatistics.getValue(NUM_ABORTED_RO_TX) +
                    localTransactionStatistics.getValue(NUM_ABORTED_WR_TX);
            long remoteCommittedTx = remoteTransactionStatistics.getValue(IspnStats.NUM_READ_ONLY_TX_COMMIT) +
                    remoteTransactionStatistics.getValue(IspnStats.NUM_COMMITTED_WR_TX);
            long remoteAbortedTx = remoteTransactionStatistics.getValue(NUM_ABORTED_RO_TX) +
                    remoteTransactionStatistics.getValue(NUM_ABORTED_WR_TX);
            long totalBornTx = localAbortedTx + localCommittedTx + remoteAbortedTx + remoteCommittedTx;
            return new Double(totalBornTx * 1.0 / convertNanosToSeconds(System.nanoTime() - this.lastResetTime));
         case THROUGHPUT:
            long totalLocalBornTx = localTransactionStatistics.getValue(IspnStats.NUM_READ_ONLY_TX_COMMIT) +
                    localTransactionStatistics.getValue(IspnStats.NUM_COMMITTED_WR_TX);
            return new Double(totalLocalBornTx * 1.0 / convertNanosToSeconds(System.nanoTime() - this.lastResetTime));
         case LOCK_HOLD_TIME_LOCAL:
            return microAvgLocal(IspnStats.NUM_HELD_LOCKS, IspnStats.LOCK_HOLD_TIME);
         case LOCK_HOLD_TIME_REMOTE:
            return microAvgRemote(IspnStats.NUM_HELD_LOCKS, IspnStats.LOCK_HOLD_TIME);
         case NUM_COMMITS:
            return new Long(localTransactionStatistics.getValue(IspnStats.NUM_READ_ONLY_TX_COMMIT) +
                    localTransactionStatistics.getValue(IspnStats.NUM_COMMITTED_WR_TX) +
                    remoteTransactionStatistics.getValue(IspnStats.NUM_READ_ONLY_TX_COMMIT) +
                    remoteTransactionStatistics.getValue(IspnStats.NUM_COMMITTED_WR_TX));
         case NUM_LOCAL_COMMITS:
            return new Long(localTransactionStatistics.getValue(IspnStats.NUM_READ_ONLY_TX_COMMIT) +
                    localTransactionStatistics.getValue(IspnStats.NUM_COMMITTED_WR_TX));
         case WRITE_SKEW_PROBABILITY:
            long totalTxs = localTransactionStatistics.getValue(IspnStats.NUM_READ_ONLY_TX_COMMIT) +
                    localTransactionStatistics.getValue(IspnStats.NUM_COMMITTED_WR_TX) +
                    localTransactionStatistics.getValue(NUM_ABORTED_RO_TX) +
                    localTransactionStatistics.getValue(NUM_ABORTED_WR_TX);
            if (totalTxs != 0) {
               long writeSkew = localTransactionStatistics.getValue(IspnStats.NUM_WRITE_SKEW);
               return new Double(writeSkew * 1.0 / totalTxs);
            }
            return new Double(0);
         case NUM_GET:
            return localTransactionStatistics.getValue(NUM_SUCCESSFUL_GETS_WR_TX) +
                    localTransactionStatistics.getValue(NUM_SUCCESSFUL_GETS_RO_TX);
         case NUM_LOCAL_REMOTE_GET:
            return localTransactionStatistics.getValue(NUM_SUCCESSFUL_REMOTE_GETS_WR_TX) +
                    localTransactionStatistics.getValue(NUM_SUCCESSFUL_REMOTE_GETS_RO_TX);
         case NUM_PUT:
            return localTransactionStatistics.getValue(NUM_SUCCESSFUL_PUTS_WR_TX);
         case NUM_REMOTE_PUT:
            return localTransactionStatistics.getValue(NUM_SUCCESSFUL_REMOTE_PUTS_WR_TX);
         case LOCAL_GET_EXECUTION:
            long num = localTransactionStatistics.getValue(IspnStats.NUM_GET) - localTransactionStatistics.getValue(IspnStats.NUM_LOCAL_REMOTE_GET) ;
            if (num == 0) {
               return new Long(0L);
            } else {
               long local_get_time = localTransactionStatistics.getValue(ALL_GET_EXECUTION) -
                       localTransactionStatistics.getValue(LOCAL_REMOTE_GET_R);

               return new Long(convertNanosToMicro(local_get_time) / num);
            }
         case WAIT_TIME_IN_COMMIT_QUEUE: {
            return microAvgLocal(NUM_WAITS_IN_COMMIT_QUEUE, WAIT_TIME_IN_COMMIT_QUEUE);
         }
         case WAIT_TIME_IN_REMOTE_COMMIT_QUEUE: {
            return microAvgRemote(NUM_WAITS_IN_COMMIT_QUEUE, WAIT_TIME_IN_COMMIT_QUEUE);
         }
         case NUM_ABORTED_TX_DUE_TO_VALIDATION: {
            return new Long(localTransactionStatistics.getValue(NUM_ABORTED_TX_DUE_TO_VALIDATION));
         }
         case NUM_KILLED_TX_DUE_TO_VALIDATION: {
            return new Long(localTransactionStatistics.getValue(NUM_KILLED_TX_DUE_TO_VALIDATION) + remoteTransactionStatistics.getValue(NUM_KILLED_TX_DUE_TO_VALIDATION));
         }
         case RO_TX_SUCCESSFUL_EXECUTION_TIME: {
            return microAvgLocal(NUM_COMMITTED_RO_TX, RO_TX_SUCCESSFUL_EXECUTION_TIME);
         }
         case SENT_SYNC_COMMIT: {
            return avgLocal(NUM_RTTS_COMMIT, SENT_SYNC_COMMIT);
         }
         case SENT_ASYNC_COMMIT: {
            return avgLocal(NUM_ASYNC_COMMIT, SENT_ASYNC_COMMIT);
         }
         case TERMINATION_COST: {
            return avgMultipleLocalCounters(TERMINATION_COST, NUM_ABORTED_WR_TX, NUM_ABORTED_RO_TX, NUM_COMMITTED_RO_TX, NUM_COMMITTED_WR_TX);
         }

         case TBC:
            return convertNanosToMicro(avgMultipleLocalCounters(IspnStats.TBC_EXECUTION_TIME, IspnStats.NUM_GET, IspnStats.NUM_PUT));
         case NTBC:
            return microAvgLocal(IspnStats.NTBC_COUNT, IspnStats.NTBC_EXECUTION_TIME);
         case RESPONSE_TIME:
            long succWrTot = convertNanosToMicro(localTransactionStatistics.getValue(IspnStats.WR_TX_SUCCESSFUL_EXECUTION_TIME));
            long abortWrTot = convertNanosToMicro(localTransactionStatistics.getValue(IspnStats.WR_TX_ABORTED_EXECUTION_TIME));
            long succRdTot = convertNanosToMicro(localTransactionStatistics.getValue(IspnStats.RO_TX_SUCCESSFUL_EXECUTION_TIME));

            long numWr = localTransactionStatistics.getValue(IspnStats.NUM_COMMITTED_WR_TX);
            long numRd = localTransactionStatistics.getValue(IspnStats.NUM_READ_ONLY_TX_COMMIT);

            if ((numWr + numRd) > 0) {
               return new Long((succRdTot + succWrTot + abortWrTot) / (numWr + numRd));
            } else {
               return new Long(0);
            }
         default:
            throw new NoIspnStatException("Invalid statistic " + param);
      }
   }

   @SuppressWarnings("UnnecessaryBoxing")
   private Long avgLocal(IspnStats counter, IspnStats duration) {
      long num = localTransactionStatistics.getValue(counter);
      if (num != 0) {
         long dur = localTransactionStatistics.getValue(duration);
         return new Long(dur / num);
      }
      return new Long(0);
   }

   @SuppressWarnings("UnnecessaryBoxing")
   private Long avgRemote(IspnStats counter, IspnStats duration) {
      long num = remoteTransactionStatistics.getValue(counter);
      if (num != 0) {
         long dur = remoteTransactionStatistics.getValue(duration);
         return new Long(dur / num);
      }
      return new Long(0);
   }

   @SuppressWarnings("UnnecessaryBoxing")
   private Double avgDoubleLocal(IspnStats counter, IspnStats duration) {
      double num = localTransactionStatistics.getValue(counter);
      if (num != 0) {
         double dur = localTransactionStatistics.getValue(duration);
         return new Double(dur / num);
      }
      return new Double(0);
   }

   @SuppressWarnings("UnnecessaryBoxing")
   private Double avgDoubleRemote(IspnStats counter, IspnStats duration) {
      double num = remoteTransactionStatistics.getValue(counter);
      if (num != 0) {
         double dur = remoteTransactionStatistics.getValue(duration);
         return new Double(dur / num);
      }
      return new Double(0);
   }

   @SuppressWarnings("UnnecessaryBoxing")
   private Long avgMultipleLocalCounters(IspnStats duration, IspnStats... counters) {
      long num = 0;
      for (IspnStats counter : counters) {
         num += localTransactionStatistics.getValue(counter);
      }
      if (num != 0) {
         long dur = localTransactionStatistics.getValue(duration);
         return new Long(dur / num);
      }
      return new Long(0);
   }

   @SuppressWarnings("UnnecessaryBoxing")
   private Long avgMultipleRemoteCounters(IspnStats duration, IspnStats... counters) {
      long num = 0;
      for (IspnStats counter : counters) {
         num += remoteTransactionStatistics.getValue(counter);
      }
      if (num != 0) {
         long dur = remoteTransactionStatistics.getValue(duration);
         return new Long(dur / num);
      }
      return new Long(0);
   }

   private static long convertNanosToMicro(long nanos) {
      return nanos / 1000;
   }

   private static long convertNanosToMillis(long nanos) {
      return nanos / 1000000;
   }

   private static long convertNanosToSeconds(long nanos) {
      return nanos / 1000000000;
   }

   private Long microAvgLocal(IspnStats counter, IspnStats duration) {
      return convertNanosToMicro(avgLocal(counter, duration));
   }

   private Long microAvgRemote(IspnStats counter, IspnStats duration) {
      return convertNanosToMicro(avgRemote(counter, duration));
   }

}
