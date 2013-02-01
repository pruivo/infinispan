package org.infinispan.stats;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.stats.percentiles.PercentileStats;
import org.infinispan.stats.percentiles.PercentileStatsFactory;
import org.infinispan.stats.translations.ExposedStatistics.IspnStats;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import static org.infinispan.stats.translations.ExposedStatistics.IspnStats.*;


/**
 * Websiste: www.cloudtm.eu
 * Date: 01/05/12
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

   public final synchronized void reset(){
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

   public NodeScopeStatisticCollector(Configuration configuration){
      this.configuration = configuration;
      reset();
   }

   public final synchronized void merge(TransactionStatistics ts){
      if (log.isTraceEnabled()) {
         log.tracef("Merge transaction statistics %s to the node statistics", ts);
      }
      if(ts instanceof LocalTransactionStatistics){
         ts.flush(this.localTransactionStatistics);
         if(ts.isCommit()){
            if(ts.isReadOnly()){
               this.localTransactionRoExecutionTime.insertSample(ts.getValue(IspnStats.RO_TX_SUCCESSFUL_EXECUTION_TIME));
            }
            else{
               this.localTransactionWrExecutionTime.insertSample(ts.getValue(IspnStats.WR_TX_SUCCESSFUL_EXECUTION_TIME));
            }
         }
		 else{
		    if(ts.isReadOnly()){
		       this.localTransactionRoExecutionTime.insertSample(ts.getValue(IspnStats.RO_TX_ABORTED_EXECUTION_TIME));
            }
            else{
		       this.localTransactionWrExecutionTime.insertSample(ts.getValue(IspnStats.WR_TX_ABORTED_EXECUTION_TIME));
            }
         }
      }
      else if(ts instanceof RemoteTransactionStatistics){
         ts.flush(this.remoteTransactionStatistics);
         if(ts.isCommit()){
            if(ts.isReadOnly()){
               this.remoteTransactionRoExecutionTime.insertSample(ts.getValue(IspnStats.RO_TX_SUCCESSFUL_EXECUTION_TIME));
            }
            else{
               this.remoteTransactionWrExecutionTime.insertSample(ts.getValue(IspnStats.WR_TX_SUCCESSFUL_EXECUTION_TIME));
            }
         }
		else{
		   if(ts.isReadOnly()){
		      this.remoteTransactionRoExecutionTime.insertSample(ts.getValue(IspnStats.RO_TX_ABORTED_EXECUTION_TIME));
		   }
		   else{
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




   public final synchronized double getPercentile(IspnStats param, int percentile) throws NoIspnStatException{
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
            throw new NoIspnStatException("Invalid percentile "+param);
      }
   }

   /*
   Can I invoke this synchronized method from inside itself??
    */

   @SuppressWarnings("UnnecessaryBoxing")
   public final synchronized Object getAttribute(IspnStats param) throws NoIspnStatException{
      if (log.isTraceEnabled()) {
         log.tracef("Get attribute %s", param);
      }
      switch (param) {
         case LOCAL_EXEC_NO_CONT:{
            long numLocalTxToPrepare = localTransactionStatistics.getValue(IspnStats.NUM_PREPARES);
            if(numLocalTxToPrepare!=0){
               long localExecNoCont = localTransactionStatistics.getValue(IspnStats.LOCAL_EXEC_NO_CONT);
               return new Long(convertNanosToMicro(localExecNoCont) / numLocalTxToPrepare);
            }
            return new Long(0);
         }
         case LOCK_HOLD_TIME:{
            long localLocks = localTransactionStatistics.getValue(IspnStats.NUM_HELD_LOCKS);
            long remoteLocks = remoteTransactionStatistics.getValue(IspnStats.NUM_HELD_LOCKS);
            if((localLocks + remoteLocks) !=0){
               long localHoldTime = localTransactionStatistics.getValue(IspnStats.LOCK_HOLD_TIME);
               long remoteHoldTime = remoteTransactionStatistics.getValue(IspnStats.LOCK_HOLD_TIME);
               return new Long(convertNanosToMicro(localHoldTime + remoteHoldTime) / (localLocks + remoteLocks));
            }
            return new Long(0);
         }
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
         case PUTS_PER_LOCAL_TX:{
            long numLocalTxToPrepare = localTransactionStatistics.getValue(IspnStats.NUM_COMMITTED_WR_TX);
            if(numLocalTxToPrepare!=0){
               long numSuccessfulPuts = localTransactionStatistics.getValue(IspnStats.NUM_SUCCESSFUL_PUTS);
               return new Long(numSuccessfulPuts / numLocalTxToPrepare);
            }
            return new Long(0);

         }
         case LOCAL_CONTENTION_PROBABILITY:{
            long numLocalPuts = localTransactionStatistics.getValue(IspnStats.NUM_PUT);
            if(numLocalPuts != 0){
               long numLocalLocalContention = localTransactionStatistics.getValue(IspnStats.LOCK_CONTENTION_TO_LOCAL);
               long numLocalRemoteContention = localTransactionStatistics.getValue(IspnStats.LOCK_CONTENTION_TO_REMOTE);
               return new Double((numLocalLocalContention + numLocalRemoteContention) * 1.0 / numLocalPuts);
            }
            return new Double(0);
         }
         case REMOTE_CONTENTION_PROBABILITY:{
            long numRemotePuts = remoteTransactionStatistics.getValue(IspnStats.NUM_PUT);
            if(numRemotePuts != 0){
               long numRemoteLocalContention = remoteTransactionStatistics.getValue(IspnStats.LOCK_CONTENTION_TO_LOCAL);
               long numRemoteRemoteContention = remoteTransactionStatistics.getValue(IspnStats.LOCK_CONTENTION_TO_REMOTE);
               return new Double((numRemoteLocalContention + numRemoteRemoteContention) * 1.0 / numRemotePuts);
            }
            return new Double(0);
         }
         case LOCK_CONTENTION_PROBABILITY:{
            long numLocalPuts = localTransactionStatistics.getValue(IspnStats.NUM_PUT);
            long numRemotePuts = remoteTransactionStatistics.getValue(IspnStats.NUM_PUT);
            long totalPuts = numLocalPuts + numRemotePuts;
            if(totalPuts!=0){
               long localLocal = localTransactionStatistics.getValue(IspnStats.LOCK_CONTENTION_TO_LOCAL);
               long localRemote = localTransactionStatistics.getValue(IspnStats.LOCK_CONTENTION_TO_REMOTE);
               long remoteLocal = remoteTransactionStatistics.getValue(IspnStats.LOCK_CONTENTION_TO_LOCAL);
               long remoteRemote = remoteTransactionStatistics.getValue(IspnStats.LOCK_CONTENTION_TO_REMOTE);
               long totalCont = localLocal + localRemote + remoteLocal + remoteRemote;
               return new Double(totalCont / totalPuts);
            }
            return new Double(0);
         }
         case COMMIT_EXECUTION_TIME:{
            long numCommits = localTransactionStatistics.getValue(IspnStats.NUM_COMMITTED_WR_TX) +
                  localTransactionStatistics.getValue(IspnStats.NUM_COMMITTED_RO_TX);
            if(numCommits!=0){
               long commitExecTime = localTransactionStatistics.getValue(IspnStats.COMMIT_EXECUTION_TIME);
               return new Long(convertNanosToMicro(commitExecTime / numCommits));
            }
            return new Long(0);

         }
         case ROLLBACK_EXECUTION_TIME:{
            long numRollbacks = localTransactionStatistics.getValue(IspnStats.NUM_ROLLBACKS);
            if(numRollbacks != 0){
               long rollbackExecTime = localTransactionStatistics.getValue(IspnStats.ROLLBACK_EXECUTION_TIME);
               return new Long(convertNanosToMicro(rollbackExecTime / numRollbacks));
            }
            return new Long(0);

         }
         case LOCK_WAITING_TIME:{
            long localWaitedForLocks = localTransactionStatistics.getValue(IspnStats.NUM_WAITED_FOR_LOCKS);
            long remoteWaitedForLocks = remoteTransactionStatistics.getValue(IspnStats.NUM_WAITED_FOR_LOCKS);
            long totalWaitedForLocks = localWaitedForLocks + remoteWaitedForLocks;
            if(totalWaitedForLocks!=0){
               long localWaitedTime = localTransactionStatistics.getValue(IspnStats.LOCK_WAITING_TIME);
               long remoteWaitedTime = remoteTransactionStatistics.getValue(IspnStats.LOCK_WAITING_TIME);
               return new Long(convertNanosToMicro(localWaitedTime + remoteWaitedTime) / totalWaitedForLocks);
            }
            return new Long(0);
         }
         case TX_WRITE_PERCENTAGE:{     //computed on the locally born txs
            long readTx = localTransactionStatistics.getValue(IspnStats.NUM_COMMITTED_RO_TX) +
                  localTransactionStatistics.getValue(NUM_ABORTED_RO_TX);
            long writeTx = localTransactionStatistics.getValue(IspnStats.NUM_COMMITTED_WR_TX) +
                  localTransactionStatistics.getValue(NUM_ABORTED_WR_TX);
            long total = readTx + writeTx;
            if(total!=0)
               return new Double(writeTx * 1.0 / total);
            return new Double(0);
         }
         case SUCCESSFUL_WRITE_PERCENTAGE:{ //computed on the locally born txs
            long readSuxTx = localTransactionStatistics.getValue(IspnStats.NUM_COMMITTED_RO_TX);
            long writeSuxTx = localTransactionStatistics.getValue(IspnStats.NUM_COMMITTED_WR_TX);
            long total = readSuxTx + writeSuxTx;
            if(total!=0){
               return new Double(writeSuxTx * 1.0 / total);
            }
            return new Double(0);
         }
         case APPLICATION_CONTENTION_FACTOR:{
            long localTakenLocks = localTransactionStatistics.getValue(IspnStats.NUM_HELD_LOCKS);
            long remoteTakenLocks = remoteTransactionStatistics.getValue(IspnStats.NUM_HELD_LOCKS);
            long elapsedTime = System.nanoTime() - this.lastResetTime;
            double totalLocksArrivalRate = (localTakenLocks + remoteTakenLocks) / convertNanosToMicro(elapsedTime);
            long holdTime = (Long)this.getAttribute(IspnStats.LOCK_HOLD_TIME);

            if((totalLocksArrivalRate*holdTime)!=0){
               double lockContProb = (Double) this.getAttribute(IspnStats.LOCK_CONTENTION_PROBABILITY);
               return new Double(lockContProb  / (totalLocksArrivalRate * holdTime));
            }
            return new Double(0);
         }
         case NUM_SUCCESSFUL_GETS_RO_TX:
            return avgLocal(IspnStats.NUM_COMMITTED_RO_TX, NUM_SUCCESSFUL_GETS_RO_TX);
         case NUM_SUCCESSFUL_GETS_WR_TX:
            return avgLocal(IspnStats.NUM_COMMITTED_WR_TX, NUM_SUCCESSFUL_GETS_WR_TX);
         case NUM_SUCCESSFUL_REMOTE_GETS_RO_TX:
            return avgLocal(IspnStats.NUM_COMMITTED_RO_TX,IspnStats.NUM_SUCCESSFUL_REMOTE_GETS_RO_TX);
         case NUM_SUCCESSFUL_REMOTE_GETS_WR_TX:
            return avgLocal(IspnStats.NUM_COMMITTED_WR_TX,IspnStats.NUM_SUCCESSFUL_REMOTE_GETS_WR_TX);
         case REMOTE_GET_EXECUTION:
            return microAvgLocal(IspnStats.NUM_REMOTE_GET, IspnStats.REMOTE_GET_EXECUTION);
         case NUM_SUCCESSFUL_PUTS_WR_TX:
            return avgLocal(IspnStats.NUM_COMMITTED_WR_TX,IspnStats.NUM_SUCCESSFUL_PUTS_WR_TX);
         case NUM_SUCCESSFUL_REMOTE_PUTS_WR_TX:
            return avgLocal(IspnStats.NUM_COMMITTED_WR_TX,IspnStats.NUM_SUCCESSFUL_REMOTE_PUTS_WR_TX);
         case REMOTE_PUT_EXECUTION:
            return microAvgLocal(IspnStats.NUM_REMOTE_PUT, IspnStats.REMOTE_PUT_EXECUTION);
         case NUM_LOCK_FAILED_DEADLOCK:
         case NUM_LOCK_FAILED_TIMEOUT:
            return new Long(localTransactionStatistics.getValue(param));
         case WR_TX_LOCAL_EXECUTION_TIME:
            return microAvgLocal(IspnStats.NUM_PREPARES, IspnStats.WR_TX_LOCAL_EXECUTION_TIME);
         case WR_TX_SUCCESSFUL_EXECUTION_TIME:
            return microAvgLocal(IspnStats.NUM_COMMITTED_WR_TX, IspnStats.WR_TX_SUCCESSFUL_EXECUTION_TIME);
		 case WR_TX_ABORTED_EXECUTION_TIME:
		    return microAvgLocal(IspnStats.NUM_ABORTED_WR_TX, IspnStats.WR_TX_ABORTED_EXECUTION_TIME);
         case RO_TX_SUCCESSFUL_EXECUTION_TIME:
            return microAvgLocal(IspnStats.NUM_COMMITTED_RO_TX, IspnStats.RO_TX_SUCCESSFUL_EXECUTION_TIME);
         case PREPARE_COMMAND_SIZE:
            return avgMultipleLocalCounters(IspnStats.PREPARE_COMMAND_SIZE, IspnStats.NUM_RTTS_PREPARE, IspnStats.NUM_ASYNC_PREPARE);
         case COMMIT_COMMAND_SIZE:
            return avgMultipleLocalCounters(IspnStats.COMMIT_COMMAND_SIZE, IspnStats.NUM_RTTS_COMMIT, IspnStats.NUM_ASYNC_COMMIT);
         case CLUSTERED_GET_COMMAND_SIZE:
            return avgLocal(IspnStats.NUM_RTTS_GET, IspnStats.CLUSTERED_GET_COMMAND_SIZE);
         case NUM_LOCK_PER_LOCAL_TX:
            return avgMultipleLocalCounters(IspnStats.NUM_HELD_LOCKS, IspnStats.NUM_COMMITTED_WR_TX, NUM_ABORTED_WR_TX);
         case NUM_LOCK_PER_REMOTE_TX:
            return avgMultipleRemoteCounters(IspnStats.NUM_HELD_LOCKS, IspnStats.NUM_COMMITTED_WR_TX, NUM_ABORTED_WR_TX);
         case NUM_LOCK_PER_SUCCESS_LOCAL_TX:
            return avgLocal(IspnStats.NUM_COMMITTED_WR_TX, IspnStats.NUM_HELD_LOCKS_SUCCESS_TX);
         case LOCAL_ROLLBACK_EXECUTION_TIME:
            return microAvgLocal(IspnStats.NUM_ROLLBACKS, IspnStats.ROLLBACK_EXECUTION_TIME);
         case REMOTE_ROLLBACK_EXECUTION_TIME:
            return microAvgRemote(IspnStats.NUM_ROLLBACKS, IspnStats.ROLLBACK_EXECUTION_TIME);
         case LOCAL_COMMIT_EXECUTION_TIME:
            return microAvgLocal(IspnStats.NUM_COMMIT_COMMAND, IspnStats.COMMIT_EXECUTION_TIME);
         case REMOTE_COMMIT_EXECUTION_TIME:
            return microAvgRemote(IspnStats.NUM_COMMIT_COMMAND, IspnStats.COMMIT_EXECUTION_TIME);
         case LOCAL_PREPARE_EXECUTION_TIME:
            return microAvgLocal(IspnStats.NUM_PREPARE_COMMAND, IspnStats.PREPARE_EXECUTION_TIME);
         case REMOTE_PREPARE_EXECUTION_TIME:
            return microAvgRemote(IspnStats.NUM_PREPARE_COMMAND, IspnStats.PREPARE_EXECUTION_TIME);
         case TX_COMPLETE_NOTIFY_EXECUTION_TIME:
            return microAvgRemote(IspnStats.NUM_TX_COMPLETE_NOTIFY_COMMAND, IspnStats.TX_COMPLETE_NOTIFY_EXECUTION_TIME);
         case ABORT_RATE:
            long totalAbort = localTransactionStatistics.getValue(NUM_ABORTED_RO_TX) +
                  localTransactionStatistics.getValue(NUM_ABORTED_WR_TX);
            long totalCommitAndAbort = localTransactionStatistics.getValue(IspnStats.NUM_COMMITTED_RO_TX) +
                  localTransactionStatistics.getValue(IspnStats.NUM_COMMITTED_WR_TX) + totalAbort;
            if (totalCommitAndAbort != 0) {
               return new Double(totalAbort * 1.0 / totalCommitAndAbort);
            }
            return new Double(0);
         case ARRIVAL_RATE:
            long localCommittedTx = localTransactionStatistics.getValue(IspnStats.NUM_COMMITTED_RO_TX) +
                  localTransactionStatistics.getValue(IspnStats.NUM_COMMITTED_WR_TX);
            long localAbortedTx = localTransactionStatistics.getValue(NUM_ABORTED_RO_TX) +
                  localTransactionStatistics.getValue(NUM_ABORTED_WR_TX);
            long remoteCommittedTx = remoteTransactionStatistics.getValue(IspnStats.NUM_COMMITTED_RO_TX) +
                  remoteTransactionStatistics.getValue(IspnStats.NUM_COMMITTED_WR_TX);
            long remoteAbortedTx = remoteTransactionStatistics.getValue(NUM_ABORTED_RO_TX) +
                  remoteTransactionStatistics.getValue(NUM_ABORTED_WR_TX);
            long totalBornTx = localAbortedTx + localCommittedTx + remoteAbortedTx + remoteCommittedTx;
            return new Double(totalBornTx * 1.0 / convertNanosToSeconds(System.nanoTime() - this.lastResetTime));
         case THROUGHPUT:
            long totalLocalBornTx = localTransactionStatistics.getValue(IspnStats.NUM_COMMITTED_RO_TX) +
                  localTransactionStatistics.getValue(IspnStats.NUM_COMMITTED_WR_TX);
            return new Double(totalLocalBornTx * 1.0 / convertNanosToSeconds(System.nanoTime() - this.lastResetTime));
         case LOCK_HOLD_TIME_LOCAL:
            return microAvgLocal(IspnStats.NUM_HELD_LOCKS,IspnStats.LOCK_HOLD_TIME);
         case LOCK_HOLD_TIME_REMOTE:
            return microAvgRemote(IspnStats.NUM_HELD_LOCKS,IspnStats.LOCK_HOLD_TIME);
         case NUM_COMMITS:
            return new Long(localTransactionStatistics.getValue(IspnStats.NUM_COMMITTED_RO_TX) +
                                  localTransactionStatistics.getValue(IspnStats.NUM_COMMITTED_WR_TX) +
                                  remoteTransactionStatistics.getValue(IspnStats.NUM_COMMITTED_RO_TX) +
                                  remoteTransactionStatistics.getValue(IspnStats.NUM_COMMITTED_WR_TX));
         case NUM_LOCAL_COMMITS:
            return new Long(localTransactionStatistics.getValue(IspnStats.NUM_COMMITTED_RO_TX) +
                                  localTransactionStatistics.getValue(IspnStats.NUM_COMMITTED_WR_TX));
         case WRITE_SKEW_PROBABILITY:
            long totalTxs = localTransactionStatistics.getValue(IspnStats.NUM_COMMITTED_RO_TX) +
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
         case NUM_REMOTE_GET:
            return localTransactionStatistics.getValue(NUM_SUCCESSFUL_REMOTE_GETS_WR_TX) +
                  localTransactionStatistics.getValue(NUM_SUCCESSFUL_REMOTE_GETS_RO_TX);
         case NUM_PUT:
            return localTransactionStatistics.getValue(NUM_SUCCESSFUL_PUTS_WR_TX);
         case NUM_REMOTE_PUT:
            return localTransactionStatistics.getValue(NUM_SUCCESSFUL_REMOTE_PUTS_WR_TX);
          case LOCAL_GET_EXECUTION:
            long num = localTransactionStatistics.getValue(IspnStats.NUM_GET);
			if(num == 0){
		       return new Long(0L);;
            }
            else{
			   long local_get_time = localTransactionStatistics.getValue(ALL_GET_EXECUTION) -
               localTransactionStatistics.getValue(RTT_GET);

               return  new Long(convertNanosToMicro(local_get_time) / num);
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
			long numRd = localTransactionStatistics.getValue(IspnStats.NUM_COMMITTED_RO_TX);

			if((numWr + numRd) > 0){
		       return (succRdTot + succWrTot + abortWrTot) / (numWr + numRd);
			}else{
			   return 0;
			}
         default:
            throw new NoIspnStatException("Invalid statistic "+param);
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

   private Long microAvgLocal(IspnStats counter, IspnStats duration){
      return convertNanosToMicro(avgLocal(counter,duration));
   }

   private Long microAvgRemote(IspnStats counter, IspnStats duration){
      return convertNanosToMicro(avgRemote(counter, duration));
   }

}
