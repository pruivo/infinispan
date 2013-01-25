package org.infinispan.stats;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.stats.translations.ExposedStatistics.IspnStats;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.HashMap;
import java.util.Map;


/**
 * @author Diego Didona <didona@gsd.inesc-id.pt>
 * @author Pedro Ruivo
 * @since 5.2
 */
public abstract class TransactionStatistics implements InfinispanStat {

   private final Log log = LogFactory.getLog(getClass());

   //Here the elements which are common for local and remote transactions
   protected long initTime;
   private boolean isReadOnly;
   private boolean isCommit;
   private String transactionalClass;
   private Map<Object, Long> takenLocks = new HashMap<Object, Long>();
   private long lastOpTimestamp;

   private final StatisticsContainer statisticsContainer;

   protected Configuration configuration;


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
   }

   public final String getTransactionalClass(){
      return this.transactionalClass;
   }

   public final void setTransactionalClass(String className){
      this.transactionalClass = className;
   }

   public final boolean isCommit(){
      return this.isCommit;
   }

   public final void setCommit(boolean commit) {
      isCommit = commit;
   }

   public final boolean isReadOnly(){
      return this.isReadOnly;
   }

   public final void setUpdateTransaction(){
      this.isReadOnly = false;
   }

   public final void addTakenLock(Object lock){
      if(!this.takenLocks.containsKey(lock))
         this.takenLocks.put(lock, System.nanoTime());
   }

   public final void addValue(IspnStats param, double value){
      try{
         int index = this.getIndex(param);
         this.statisticsContainer.addValue(index,value);
         if (log.isTraceEnabled()) {
            log.tracef("Add %s to %s", value, param);
         }
      } catch(NoIspnStatException e){
         log.warnf(e, "Exception caught when trying to add the value %s to %s.", value, param);
      }
   }

   public final long getValue(IspnStats param){
      int index = this.getIndex(param);
      long value = this.statisticsContainer.getValue(index);
      if (log.isTraceEnabled()) {
         log.tracef("Value of %s is %s", param, value);
      }
      return value;
   }

   public final void incrementValue(IspnStats param){
      this.addValue(param,1);
   }

   public final void terminateTransaction() {
      if (log.isTraceEnabled()) {
         log.tracef("Terminating transaction. Is read only? %s. Is commit? %s", isReadOnly, isCommit);
      }
      long now = System.nanoTime();
      double execTime = now - this.initTime;
      if(this.isReadOnly){
         if(isCommit){
            this.incrementValue(IspnStats.NUM_COMMITTED_RO_TX);
            this.addValue(IspnStats.RO_TX_SUCCESSFUL_EXECUTION_TIME,execTime);
            this.addValue(IspnStats.NUM_SUCCESSFUL_GETS_RO_TX, this.getValue(IspnStats.NUM_GET));
            this.addValue(IspnStats.NUM_SUCCESSFUL_REMOTE_GETS_RO_TX, this.getValue(IspnStats.NUM_REMOTE_GET));
         } else{
            this.incrementValue(IspnStats.NUM_ABORTED_RO_TX);
            this.addValue(IspnStats.RO_TX_ABORTED_EXECUTION_TIME,execTime);
         }
      } else{
         if(isCommit){
            this.incrementValue(IspnStats.NUM_COMMITTED_WR_TX);
            this.addValue(IspnStats.WR_TX_SUCCESSFUL_EXECUTION_TIME,execTime);
            this.addValue(IspnStats.NUM_SUCCESSFUL_GETS_WR_TX, this.getValue(IspnStats.NUM_GET));
            this.addValue(IspnStats.NUM_SUCCESSFUL_REMOTE_GETS_WR_TX, this.getValue(IspnStats.NUM_REMOTE_GET));
            this.addValue(IspnStats.NUM_SUCCESSFUL_PUTS_WR_TX, this.getValue(IspnStats.NUM_PUT));
            this.addValue(IspnStats.NUM_SUCCESSFUL_REMOTE_PUTS_WR_TX, this.getValue(IspnStats.NUM_REMOTE_PUT));
         } else{
            this.incrementValue(IspnStats.NUM_ABORTED_WR_TX);
            this.addValue(IspnStats.WR_TX_ABORTED_EXECUTION_TIME,execTime);
         }
      }

      int heldLocks = this.takenLocks.size();
      double cumulativeLockHoldTime = this.computeCumulativeLockHoldTime(heldLocks,now);
      this.addValue(IspnStats.NUM_HELD_LOCKS,heldLocks);
      this.addValue(IspnStats.LOCK_HOLD_TIME,cumulativeLockHoldTime);

      terminate();
   }

   public final void flush(TransactionStatistics ts){
      if (log.isTraceEnabled()) {
         log.tracef("Flush this [%s] to %s", this, ts);
      }
      this.statisticsContainer.mergeTo(ts.statisticsContainer);
   }

   public final void dump(){
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

   private long computeCumulativeLockHoldTime(int numLocks,long currentTime){
      long ret = numLocks * currentTime;
      for(Object o:this.takenLocks.keySet())
         ret-=this.takenLocks.get(o);
      return ret;
   }

   public void setLastOpTimestamp(long lastOpTimestamp) {
      this.lastOpTimestamp = lastOpTimestamp;
   }

   public long getLastOpTimestamp() {
      return lastOpTimestamp;
   }
}

