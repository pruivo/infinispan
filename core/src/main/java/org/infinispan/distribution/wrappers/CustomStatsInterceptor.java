package org.infinispan.distribution.wrappers;

import org.infinispan.commands.SetClassCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.base.BaseCustomInterceptor;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.remoting.InboundInvocationHandler;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.CommandAwareRpcDispatcher;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.stats.TransactionsStatisticsRegistry;
import org.infinispan.stats.translations.ExposedStatistics.IspnStats;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.WriteSkewException;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.locks.DeadlockDetectedException;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.rhq.helpers.pluginAnnotations.agent.Metric;
import org.rhq.helpers.pluginAnnotations.agent.Operation;

import java.lang.reflect.Field;

/**
 * Massive hack for a noble cause!
 *
 * @author Mircea Markus <mircea.markus@jboss.com> (C) 2011 Red Hat Inc.
 * @author Diego Didona <didona@gsd.inesc-id.pt>
 * @author Pedro Ruivo
 * @since 5.2
 */
@MBean(objectName = "ExtendedStatistics", description = "Component that manages and exposes extended statistics " +
      "relevant to transactions.")
public abstract class CustomStatsInterceptor extends BaseCustomInterceptor {
   //TODO what about the transaction implicit vs transaction explicit? should we take in account this and ignore
   //the implicit stuff?

   private final Log log = LogFactory.getLog(getClass());

   private TransactionTable transactionTable;
   private Configuration configuration;
   private RpcManagerWrapper rpcManagerWrapper;
   private DistributionManager distributionManager;

   @Inject
   public void inject(TransactionTable transactionTable) {
      this.transactionTable = transactionTable;
   }

   @Start(priority = 99)
   public void start(){
      // we want that this method is the last to be invoked, otherwise the start method is not invoked
      // in the real components
      replace();
      log.info("Initializing the TransactionStatisticsRegistry");
      TransactionsStatisticsRegistry.init(this.configuration);
      distributionManager = cache.getAdvancedCache().getDistributionManager();
   }

   @Override
   public Object visitSetClassCommand(InvocationContext ctx, SetClassCommand command) throws Throwable {
      log.tracef("visitSetClassCommand invoked");
      Object ret;
      if(ctx.isInTxScope()){
         this.initStatsIfNecessary(ctx);
      }
      TransactionsStatisticsRegistry.setTransactionalClass(command.getTransactionalClass());
      //TransactionsStatisticsRegistry.putThreadClasses(Thread.currentThread().getId(), command.getTransactionalClass());
      //System.out.println(threadClasses.get(Thread.currentThread().getId()));
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      if (log.isTraceEnabled()) {
         log.tracef("Visit Put Key Value command %s. Is it in transaction scope? %s. Is it local? %s", command,
                    ctx.isInTxScope(), ctx.isOriginLocal());
      }
      Object ret;
      if(ctx.isInTxScope()){
         this.initStatsIfNecessary(ctx);
         TransactionsStatisticsRegistry.setUpdateTransaction();
         long currTime = System.nanoTime();
         TransactionsStatisticsRegistry.incrementValue(IspnStats.NUM_PUT);
         TransactionsStatisticsRegistry.addNTBCValue(currTime);
         try {
            ret =  invokeNextInterceptor(ctx,command);
         } catch (TimeoutException e) {
            if (ctx.isOriginLocal() && isLockTimeout(e)) {
               TransactionsStatisticsRegistry.incrementValue(IspnStats.NUM_LOCK_FAILED_TIMEOUT);
            }
            throw e;
         } catch (DeadlockDetectedException e) {
            if (ctx.isOriginLocal()) {
               TransactionsStatisticsRegistry.incrementValue(IspnStats.NUM_LOCK_FAILED_DEADLOCK);
            }
            throw e;
         } catch (WriteSkewException e) {
            if (ctx.isOriginLocal()) {
               TransactionsStatisticsRegistry.incrementValue(IspnStats.NUM_WRITE_SKEW);
            }
            throw e;
         }
         if(isRemote(command.getKey())){
            TransactionsStatisticsRegistry.addValue(IspnStats.REMOTE_PUT_EXECUTION,System.nanoTime() - currTime);
            TransactionsStatisticsRegistry.incrementValue(IspnStats.NUM_REMOTE_PUT);
         }
         TransactionsStatisticsRegistry.setLastOpTimestamp(System.nanoTime());
         return ret;
      }
      else
         return invokeNextInterceptor(ctx,command);
   }

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable{
      if (log.isTraceEnabled()) {
         log.tracef("Visit Get Key Value command %s. Is it in transaction scope? %s. Is it local? %s", command,
                    ctx.isInTxScope(), ctx.isOriginLocal());
      }
      boolean isTx = ctx.isInTxScope();
      Object ret;
      if(isTx){
         long currTimeForAllGetCommand = System.nanoTime();
         TransactionsStatisticsRegistry.addNTBCValue(currTimeForAllGetCommand);
         this.initStatsIfNecessary(ctx);
         long currTime = 0;
         boolean isRemoteKey = isRemote(command.getKey());
         if(isRemoteKey){
            currTime = System.nanoTime();
         }

         ret = invokeNextInterceptor(ctx,command);
         long lastTimeOp = System.nanoTime();
         if(isRemoteKey){
            TransactionsStatisticsRegistry.incrementValue(IspnStats.NUM_REMOTE_GET);
            TransactionsStatisticsRegistry.addValue(IspnStats.REMOTE_GET_EXECUTION, lastTimeOp - currTime);
         }
         TransactionsStatisticsRegistry.addValue(IspnStats.ALL_GET_EXECUTION, lastTimeOp - currTimeForAllGetCommand);
         TransactionsStatisticsRegistry.incrementValue(IspnStats.NUM_GET);
         TransactionsStatisticsRegistry.setLastOpTimestamp(lastTimeOp);
      }
      else{
         ret = invokeNextInterceptor(ctx,command);
      }
      return ret;
   }

   protected boolean isRemote(Object key){
      //Why?!?!
      return false;
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      if (log.isTraceEnabled()) {
         log.tracef("Visit Commit command %s. Is it local?. Transaction is %s", command,
                    ctx.isOriginLocal(), command.getGlobalTransaction().prettyPrint());
      }
      this.initStatsIfNecessary(ctx);
      long currTime = System.nanoTime();
      TransactionsStatisticsRegistry.addNTBCValue(currTime);
      Object ret = invokeNextInterceptor(ctx,command);
      updateTime(IspnStats.COMMIT_EXECUTION_TIME, IspnStats.NUM_COMMIT_COMMAND, currTime);
      TransactionsStatisticsRegistry.setTransactionOutcome(true);
      if (ctx.isOriginLocal()) {
         TransactionsStatisticsRegistry.terminateTransaction();
      }
      return ret;
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      if (log.isTraceEnabled()) {
         log.tracef("Visit Prepare command %s. Is it local?. Transaction is %s", command,
                    ctx.isOriginLocal(), command.getGlobalTransaction().prettyPrint());
      }
      this.initStatsIfNecessary(ctx);
      TransactionsStatisticsRegistry.onPrepareCommand();
      if (command.hasModifications()) {
         TransactionsStatisticsRegistry.setUpdateTransaction();
      }

      boolean success = false;
      try {
         long currTime = System.nanoTime();
         Object ret = invokeNextInterceptor(ctx,command);
         updateTime(IspnStats.PREPARE_EXECUTION_TIME, IspnStats.NUM_PREPARE_COMMAND, currTime);
         success = true;
         return ret;
      } catch (TimeoutException e) {
         if (ctx.isOriginLocal() && isLockTimeout(e)) {
            TransactionsStatisticsRegistry.incrementValue(IspnStats.NUM_LOCK_FAILED_TIMEOUT);
         }
         throw e;
      } catch (DeadlockDetectedException e) {
         if (ctx.isOriginLocal()) {
            TransactionsStatisticsRegistry.incrementValue(IspnStats.NUM_LOCK_FAILED_DEADLOCK);
         }
         throw e;
      } catch (WriteSkewException e) {
         if (ctx.isOriginLocal()) {
            TransactionsStatisticsRegistry.incrementValue(IspnStats.NUM_WRITE_SKEW);
         }
         throw e;
      } finally {
         if (command.isOnePhaseCommit()) {
            TransactionsStatisticsRegistry.setTransactionOutcome(success);
            if (ctx.isOriginLocal()) {
               TransactionsStatisticsRegistry.terminateTransaction();
            }
         }
      }
   }


   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable{
      if (log.isTraceEnabled()) {
         log.tracef("Visit Rollback command %s. Is it local?. Transaction is %s", command,
                    ctx.isOriginLocal(), command.getGlobalTransaction().prettyPrint());
      }
      this.initStatsIfNecessary(ctx);
      long initRollbackTime = System.nanoTime();
      Object ret = invokeNextInterceptor(ctx,command);
      updateTime(IspnStats.ROLLBACK_EXECUTION_TIME, IspnStats.NUM_ROLLBACKS, initRollbackTime);
      TransactionsStatisticsRegistry.setTransactionOutcome(false);
      if (ctx.isOriginLocal()) {
         TransactionsStatisticsRegistry.terminateTransaction();
      }
      return ret;
   }

   private void replace(){
      log.info("CustomStatsInterceptor Enabled!");
      ComponentRegistry componentRegistry = cache.getAdvancedCache().getComponentRegistry();

      GlobalComponentRegistry globalComponentRegistry = componentRegistry.getGlobalComponentRegistry();
      InboundInvocationHandlerWrapper invocationHandlerWrapper = rewireInvocationHandler(globalComponentRegistry);
      globalComponentRegistry.rewire();

      replaceFieldInTransport(componentRegistry, invocationHandlerWrapper);

      replaceRpcManager(componentRegistry);
      replaceLockManager(componentRegistry);
      componentRegistry.rewire();

      this.wireConfiguration();
   }

   private void wireConfiguration(){
      this.configuration = cache.getAdvancedCache().getCacheConfiguration();
   }

   private void replaceFieldInTransport(ComponentRegistry componentRegistry, InboundInvocationHandlerWrapper invocationHandlerWrapper) {
      JGroupsTransport t = (JGroupsTransport) componentRegistry.getComponent(Transport.class);
      CommandAwareRpcDispatcher card = t.getCommandAwareRpcDispatcher();
      try {
         Field f = card.getClass().getDeclaredField("inboundInvocationHandler");
         f.setAccessible(true);
         f.set(card, invocationHandlerWrapper);
      } catch (NoSuchFieldException e) {
         e.printStackTrace();
      } catch (IllegalAccessException e) {
         e.printStackTrace();
      }
   }

   private InboundInvocationHandlerWrapper rewireInvocationHandler(GlobalComponentRegistry globalComponentRegistry) {
      InboundInvocationHandler inboundHandler = globalComponentRegistry.getComponent(InboundInvocationHandler.class);
      InboundInvocationHandlerWrapper invocationHandlerWrapper = new InboundInvocationHandlerWrapper(inboundHandler,
                                                                                                     transactionTable);
      globalComponentRegistry.registerComponent(invocationHandlerWrapper, InboundInvocationHandler.class);
      return invocationHandlerWrapper;
   }

   private void replaceLockManager(ComponentRegistry componentRegistry) {
      LockManager lockManager = componentRegistry.getComponent(LockManager.class);
      LockManagerWrapper lockManagerWrapper = new LockManagerWrapper(lockManager);
      componentRegistry.registerComponent(lockManagerWrapper, LockManager.class);
   }

   private void replaceRpcManager(ComponentRegistry componentRegistry) {
      RpcManager rpcManager = componentRegistry.getComponent(RpcManager.class);
      RpcManagerWrapper rpcManagerWrapper = new RpcManagerWrapper(rpcManager);
      componentRegistry.registerComponent(rpcManagerWrapper, RpcManager.class);
      this.rpcManagerWrapper = rpcManagerWrapper;
   }

   private void initStatsIfNecessary(InvocationContext ctx){
      if(ctx.isInTxScope())
         TransactionsStatisticsRegistry.initTransactionIfNecessary((TxInvocationContext) ctx);
   }

   private boolean isLockTimeout(TimeoutException e) {
      return e.getMessage().startsWith("Unable to acquire lock after");
   }

   private void updateTime(IspnStats duration, IspnStats counter, long initTime) {
      TransactionsStatisticsRegistry.addValue(duration, System.nanoTime() - initTime);
      TransactionsStatisticsRegistry.incrementValue(counter);
   }

   //JMX exposed methods

	private long handleLong(Long object){

      if(object == null){
         return new Long(0L);
      }
      else{
         return object;
      }

   }

   private double handleDouble(Double object){

      if(object == null){
         return new Double(0D);
      }
      else{
         return object;
      }

   }

   @ManagedAttribute(description = "Average number of puts performed by a successful local transaction")
   @Metric(displayName = "Number of puts")
   public long getAvgNumPutsBySuccessfulLocalTx(){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.PUTS_PER_LOCAL_TX));
   }

   @ManagedAttribute(description = "Average Prepare Round-Trip Time duration (in microseconds)")
   @Metric(displayName = "Average Prepare RTT")
   public long getAvgPrepareRtt() {
      return handleLong((Long)(TransactionsStatisticsRegistry.getAttribute((IspnStats.RTT_PREPARE))));
   }

   @ManagedAttribute(description = "Average Commit Round-Trip Time duration (in microseconds)")
   @Metric(displayName = "Average Commit RTT")
   public long getAvgCommitRtt() {
      return handleLong((Long)(TransactionsStatisticsRegistry.getAttribute((IspnStats.RTT_COMMIT))));
   }

   @ManagedAttribute(description = "Average Remote Get Round-Trip Time duration (in microseconds)")
   @Metric(displayName = "Average Remote Get RTT")
   public long getAvgRemoteGetRtt() {
      return handleLong((Long)(TransactionsStatisticsRegistry.getAttribute((IspnStats.RTT_GET))));
   }

   @ManagedAttribute(description = "Average Rollback Round-Trip Time duration (in microseconds)")
   @Metric(displayName = "Average Rollback RTT")
   public long getAvgRollbackRtt() {
      return handleLong((Long)(TransactionsStatisticsRegistry.getAttribute((IspnStats.RTT_ROLLBACK))));
   }

   @ManagedAttribute(description = "Average asynchronous Prepare duration (in microseconds)")
   @Metric(displayName = "Average Prepare Async")
   public long getAvgPrepareAsync() {
      return handleLong((Long)(TransactionsStatisticsRegistry.getAttribute((IspnStats.ASYNC_PREPARE))));
   }

   @ManagedAttribute(description = "Average asynchronous Commit duration (in microseconds)")
   @Metric(displayName = "Average Commit Async")
   public long getAvgCommitAsync() {
      return handleLong((Long)(TransactionsStatisticsRegistry.getAttribute((IspnStats.ASYNC_COMMIT))));
   }

   @ManagedAttribute(description = "Average asynchronous Complete Notification duration (in microseconds)")
   @Metric(displayName = "Average Complete Notification Async")
   public long getAvgCompleteNotificationAsync() {
      return handleLong((Long)(TransactionsStatisticsRegistry.getAttribute((IspnStats.ASYNC_COMPLETE_NOTIFY))));
   }

   @ManagedAttribute(description = "Average asynchronous Rollback duration (in microseconds)")
   @Metric(displayName = "Average Rollback Async")
   public long getAvgRollbackAsync() {
      return handleLong((Long)(TransactionsStatisticsRegistry.getAttribute((IspnStats.ASYNC_ROLLBACK))));
   }

   @ManagedAttribute(description = "Average number of nodes in Commit destination set")
   @Metric(displayName = "Average Number of Nodes in Commit Destination Set")
   public long getAvgNumNodesCommit() {
      return handleLong((Long)(TransactionsStatisticsRegistry.getAttribute((IspnStats.NUM_NODES_COMMIT))));
   }

   @ManagedAttribute(description = "Average number of nodes in Complete Notification destination set")
   @Metric(displayName = "Average Number of Nodes in Complete Notification Destination Set")
   public long getAvgNumNodesCompleteNotification() {
      return handleLong((Long)(TransactionsStatisticsRegistry.getAttribute((IspnStats.NUM_NODES_COMPLETE_NOTIFY))));
   }

   @ManagedAttribute(description = "Average number of nodes in Remote Get destination set")
   @Metric(displayName = "Average Number of Nodes in Remote Get Destination Set")
   public long getAvgNumNodesRemoteGet() {
      return handleLong((Long)(TransactionsStatisticsRegistry.getAttribute((IspnStats.NUM_NODES_GET))));
   }

   @ManagedAttribute(description = "Average number of nodes in Prepare destination set")
   @Metric(displayName = "Average Number of Nodes in Prepare Destination Set")
   public long getAvgNumNodesPrepare() {
      return handleLong((Long)(TransactionsStatisticsRegistry.getAttribute((IspnStats.NUM_NODES_PREPARE))));
   }

   @ManagedAttribute(description = "Average number of nodes in Rollback destination set")
   @Metric(displayName = "Average Number of Nodes in Rollback Destination Set")
   public long getAvgNumNodesRollback() {
      return handleLong((Long)(TransactionsStatisticsRegistry.getAttribute((IspnStats.NUM_NODES_ROLLBACK))));
   }

   @ManagedAttribute(description = "Application Contention Factor")
   @Metric(displayName = "Application Contention Factor")
   public double getApplicationContentionFactor() {
      return handleDouble((Double)TransactionsStatisticsRegistry.getAttribute((IspnStats.APPLICATION_CONTENTION_FACTOR)));
   }

   @Deprecated
   @ManagedAttribute(description = "Local Contention Probability")
   @Metric(displayName = "Local Conflict Probability")
   public double getLocalContentionProbability(){
      return handleDouble((Double)TransactionsStatisticsRegistry.getAttribute((IspnStats.LOCAL_CONTENTION_PROBABILITY)));
   }

   @ManagedAttribute(description = "Remote Contention Probability")
   @Metric(displayName = "Remote Conflict Probability")
   public double getRemoteContentionProbability(){
      return handleDouble((Double)TransactionsStatisticsRegistry.getAttribute((IspnStats.REMOTE_CONTENTION_PROBABILITY)));
   }

   @ManagedAttribute(description = "Lock Contention Probability")
   @Metric(displayName = "Lock Contention Probability")
   public double getLockContentionProbability(){
      return handleDouble((Double)TransactionsStatisticsRegistry.getAttribute((IspnStats.LOCK_CONTENTION_PROBABILITY)));
   }

   @ManagedAttribute(description = "Local execution time of a transaction without the time waiting for lock acquisition")
   @Metric(displayName = "Local Execution Time Without Locking Time")
   public long getLocalExecutionTimeWithoutLock(){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.LOCAL_EXEC_NO_CONT));
   }

   @ManagedAttribute(description = "Average lock holding time (in microseconds)")
   @Metric(displayName = "Average Lock Holding Time")
   public long getAvgLockHoldTime(){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.LOCK_HOLD_TIME));
   }

   @ManagedAttribute(description = "Average lock local holding time (in microseconds)")
   @Metric(displayName = "Average Lock Local Holding Time")
   public long getAvgLocalLockHoldTime(){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.LOCK_HOLD_TIME_LOCAL));
   }

   @ManagedAttribute(description = "Average lock remote holding time (in microseconds)")
   @Metric(displayName = "Average Lock Remote Holding Time")
   public long getAvgRemoteLockHoldTime(){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.LOCK_HOLD_TIME_REMOTE));
   }

   @ManagedAttribute(description = "Average local commit duration time (2nd phase only) (in microseconds)")
   @Metric(displayName = "Average Commit Time")
   public long getAvgCommitTime(){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.COMMIT_EXECUTION_TIME));
   }

   @ManagedAttribute(description = "Average local rollback duration time (2nd phase only) (in microseconds)")
   @Metric(displayName = "Average Rollback Time")
   public long getAvgRollbackTime(){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.ROLLBACK_EXECUTION_TIME));
   }

   @ManagedAttribute(description = "Average prepare command size (in bytes)")
   @Metric(displayName = "Average Prepare Command Size")
   public long getAvgPrepareCommandSize(){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.PREPARE_COMMAND_SIZE));
   }

   @ManagedAttribute(description = "Average commit command size (in bytes)")
   @Metric(displayName = "Average Commit Command Size")
   public long getAvgCommitCommandSize(){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.COMMIT_COMMAND_SIZE));
   }

   @ManagedAttribute(description = "Average clustered get command size (in bytes)")
   @Metric(displayName = "Average Clustered Get Command Size")
   public long getAvgClusteredGetCommandSize(){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.CLUSTERED_GET_COMMAND_SIZE));
   }

   @ManagedAttribute(description = "Average time waiting for the lock acquisition (in microseconds)")
   @Metric(displayName = "Average Lock Waiting Time")
   public long getAvgLockWaitingTime(){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.LOCK_WAITING_TIME));
   }

   @ManagedAttribute(description = "Average transaction arrival rate, originated locally and remotely (in transaction " +
         "per second)")
   @Metric(displayName = "Average Transaction Arrival Rate")
   public double getAvgTxArrivalRate(){
      return handleDouble((Double)TransactionsStatisticsRegistry.getAttribute(IspnStats.ARRIVAL_RATE));
   }

   @ManagedAttribute(description = "Percentage of Write transaction executed locally (committed and aborted)")
   @Metric(displayName = "Percentage of Write Transactions")
   public double getPercentageWriteTransactions(){
      return handleDouble((Double)TransactionsStatisticsRegistry.getAttribute(IspnStats.TX_WRITE_PERCENTAGE));
   }

   @ManagedAttribute(description = "Percentage of Write transaction executed in all successfully executed " +
         "transactions (local transaction only)")
   @Metric(displayName = "Percentage of Successfully Write Transactions")
   public double getPercentageSuccessWriteTransactions(){
      return handleDouble((Double)TransactionsStatisticsRegistry.getAttribute(IspnStats.SUCCESSFUL_WRITE_PERCENTAGE));
   }

   @ManagedAttribute(description = "The number of aborted transactions due to timeout in lock acquisition")
   @Metric(displayName = "Number of Aborted Transaction due to Lock Acquisition Timeout")
   public long getNumAbortedTxDueTimeout(){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_LOCK_FAILED_TIMEOUT));
   }

   @ManagedAttribute(description = "The number of aborted transactions due to deadlock")
   @Metric(displayName = "Number of Aborted Transaction due to Deadlock")
   public long getNumAbortedTxDueDeadlock(){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_LOCK_FAILED_DEADLOCK));
   }

   @ManagedAttribute(description = "Average successful read-only transaction duration (in microseconds)")
   @Metric(displayName = "Average Read-Only Transaction Duration")
   public long getAvgReadOnlyTxDuration(){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.RO_TX_SUCCESSFUL_EXECUTION_TIME));
   }

   @ManagedAttribute(description = "Average successful write transaction duration (in microseconds)")
   @Metric(displayName = "Average Write Transaction Duration")
   public long getAvgWriteTxDuration(){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.WR_TX_SUCCESSFUL_EXECUTION_TIME));
   }

   @ManagedAttribute(description = "Average aborted write transaction duration (in microseconds)")
   @Metric(displayName = "Average Aborted Write Transaction Duration")
   public long getAvgAbortedWriteTxDuration(){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.WR_TX_ABORTED_EXECUTION_TIME));
   }



   @ManagedAttribute(description = "Average write transaction local execution time (in microseconds)")
   @Metric(displayName = "Average Write Transaction Local Execution Time")
   public long getAvgWriteTxLocalExecution(){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.WR_TX_LOCAL_EXECUTION_TIME));
   }

   @ManagedAttribute(description = "Average number of locks per write local transaction")
   @Metric(displayName = "Average Number of Lock per Local Transaction")
   public long getAvgNumOfLockLocalTx(){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_LOCK_PER_LOCAL_TX));
   }

   @ManagedAttribute(description = "Average number of locks per write remote transaction")
   @Metric(displayName = "Average Number of Lock per Remote Transaction")
   public long getAvgNumOfLockRemoteTx(){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_LOCK_PER_REMOTE_TX));
   }

   @ManagedAttribute(description = "Average number of locks per successfully write local transaction")
   @Metric(displayName = "Average Number of Lock per Successfully Local Transaction")
   public long getAvgNumOfLockSuccessLocalTx(){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_LOCK_PER_SUCCESS_LOCAL_TX));
   }

   @ManagedAttribute(description = "Average time it takes to execute the prepare command locally (in microseconds)")
   @Metric(displayName = "Average Local Prepare Execution Time")
   public long getAvgLocalPrepareTime(){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.LOCAL_PREPARE_EXECUTION_TIME));
   }

   @ManagedAttribute(description = "Average time it takes to execute the prepare command remotely (in microseconds)")
   @Metric(displayName = "Average Remote Prepare Execution Time")
   public long getAvgRemotePrepareTime(){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.REMOTE_PREPARE_EXECUTION_TIME));
   }

   @ManagedAttribute(description = "Average time it takes to execute the commit command locally (in microseconds)")
   @Metric(displayName = "Average Local Commit Execution Time")
   public long getAvgLocalCommitTime(){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.LOCAL_COMMIT_EXECUTION_TIME));
   }

   @ManagedAttribute(description = "Average time it takes to execute the commit command remotely (in microseconds)")
   @Metric(displayName = "Average Remote Commit Execution Time")
   public long getAvgRemoteCommitTime(){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.REMOTE_COMMIT_EXECUTION_TIME));
   }

   @ManagedAttribute(description = "Average time it takes to execute the rollback command locally (in microseconds)")
   @Metric(displayName = "Average Local Rollback Execution Time")
   public long getAvgLocalRollbackTime(){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.LOCAL_ROLLBACK_EXECUTION_TIME));
   }

   @ManagedAttribute(description = "Average time it takes to execute the rollback command remotely (in microseconds)")
   @Metric(displayName = "Average Remote Rollback Execution Time")
   public long getAvgRemoteRollbackTime(){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.REMOTE_ROLLBACK_EXECUTION_TIME));
   }

   @ManagedAttribute(description = "Average time it takes to execute the rollback command remotely (in microseconds)")
   @Metric(displayName = "Average Remote Transaction Completion Notify Execution Time")
   public long getAvgRemoteTxCompleteNotifyTime(){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.TX_COMPLETE_NOTIFY_EXECUTION_TIME));
   }

   @ManagedAttribute(description = "Abort Rate")
   @Metric(displayName = "Abort Rate")
   public double getAbortRate(){
      return handleDouble((Double)TransactionsStatisticsRegistry.getAttribute(IspnStats.ABORT_RATE));
   }

   @ManagedAttribute(description = "Throughput (in transactions per second)")
   @Metric(displayName = "Throughput")
   public double getThroughput(){
      return handleDouble((Double)TransactionsStatisticsRegistry.getAttribute(IspnStats.THROUGHPUT));
   }

   @ManagedAttribute(description = "Average number of get operations per (local) read-only transaction")
   @Operation(displayName = "Average number of get operations per (local) read-only transaction")
   public long getAvgGetsPerROTransaction(){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_SUCCESSFUL_GETS_RO_TX));
   }

   @ManagedAttribute(description = "Average number of get operations per (local) read-write transaction")
   @Operation(displayName = "Average number of get operations per (local) read-write transaction")
   public long getAvgGetsPerWrTransaction(){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_SUCCESSFUL_GETS_WR_TX));
   }

   @ManagedAttribute(description = "Average number of remote get operations per (local) read-write transaction")
   @Operation(displayName = "Average number of remote get operations per (local) read-write transaction")
   public long getAvgRemoteGetsPerWrTransaction(){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_SUCCESSFUL_REMOTE_GETS_WR_TX));
   }

   @ManagedAttribute(description = "Average number of remote get operations per (local) read-only transaction")
   @Operation(displayName = "Average number of remote get operations per (local) read-only transaction")
   public long getAvgRemoteGetsPerROTransaction(){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_SUCCESSFUL_REMOTE_GETS_RO_TX));
   }

   @ManagedAttribute(description = "Average cost of a remote get")
   @Operation(displayName = "Remote get cost")
   public long getRemoteGetExecutionTime(){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.REMOTE_GET_EXECUTION));
   }

   @ManagedAttribute(description = "Average number of put operations per (local) read-write transaction")
   @Operation(displayName = "Average number of put operations per (local) read-write transaction")
   public long getAvgPutsPerWrTransaction(){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_SUCCESSFUL_PUTS_WR_TX));
   }

   @ManagedAttribute(description = "Average number of remote put operations per (local) read-write transaction")
   @Operation(displayName = "Average number of remote put operations per (local) read-write transaction")
   public long getAvgRemotePutsPerWrTransaction(){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_SUCCESSFUL_REMOTE_PUTS_WR_TX));
   }

   @ManagedAttribute(description = "Average cost of a remote put")
   @Operation(displayName = "Remote put cost")
   public long getRemotePutExecutionTime(){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.REMOTE_PUT_EXECUTION));
   }

   @ManagedAttribute(description = "Number of gets performed since last reset")
   @Operation(displayName = "Number of Gets")
   public long getNumberOfGets(){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_GET));
   }

   @ManagedAttribute(description = "Number of remote gets performed since last reset")
   @Operation(displayName = "Number of Remote Gets")
   public long getNumberOfRemoteGets(){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_REMOTE_GET));
   }

   @ManagedAttribute(description = "Number of puts performed since last reset")
   @Operation(displayName = "Number of Puts")
   public long getNumberOfPuts(){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_PUT));
   }

   @ManagedAttribute(description = "Number of remote puts performed since last reset")
   @Operation(displayName = "Number of Remote Puts")
   public long getNumberOfRemotePuts(){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_REMOTE_PUT));
   }

   @ManagedAttribute(description = "Number of committed transactions since last reset")
   @Operation(displayName = "Number Of Commits")
   public long getNumberOfCommits() {
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_COMMITS));
   }

   @ManagedAttribute(description = "Number of local committed transactions since last reset")
   @Operation(displayName = "Number Of Local Commits")
   public long getNumberOfLocalCommits() {
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_LOCAL_COMMITS));
   }

   @ManagedAttribute(description = "Write skew probability")
   @Operation(displayName = "Write Skew Probability")
   public double getWriteSkewProbability() {
      return handleDouble((Double)TransactionsStatisticsRegistry.getAttribute(IspnStats.WRITE_SKEW_PROBABILITY));
   }

   @ManagedOperation(description = "K-th percentile of local read-only transactions execution time")
   @Operation(displayName = "K-th Percentile Local Read-Only Transactions")
   public double getPercentileLocalReadOnlyTransaction(int percentile){
      return handleDouble((Double)TransactionsStatisticsRegistry.getPercentile(IspnStats.RO_LOCAL_PERCENTILE, percentile));
   }

   @ManagedOperation(description = "K-th percentile of remote read-only transactions execution time")
   @Operation(displayName = "K-th Percentile Remote Read-Only Transactions")
   public double getPercentileRemoteReadOnlyTransaction(int percentile){
      return handleDouble((Double)TransactionsStatisticsRegistry.getPercentile(IspnStats.RO_REMOTE_PERCENTILE, percentile));
   }

   @ManagedOperation(description = "K-th percentile of local write transactions execution time")
   @Operation(displayName = "K-th Percentile Local Write Transactions")
   public double getPercentileLocalRWriteTransaction(int percentile){
      return handleDouble((Double)TransactionsStatisticsRegistry.getPercentile(IspnStats.WR_LOCAL_PERCENTILE, percentile));
   }

   @ManagedOperation(description = "K-th percentile of remote write transactions execution time")
   @Operation(displayName = "K-th Percentile Remote Write Transactions")
   public double getPercentileRemoteWriteTransaction(int percentile){
      return handleDouble((Double)TransactionsStatisticsRegistry.getPercentile(IspnStats.WR_REMOTE_PERCENTILE, percentile));
   }

   @ManagedOperation(description = "Reset all the statistics collected")
   @Operation(displayName = "Reset All Statistics")
   public void resetStatistics(){
      TransactionsStatisticsRegistry.reset();
   }

   @ManagedAttribute(description = "Average Local processing Get time (in microseconds)")
   @Metric(displayName = "Average Local Get time")
   public long getAvgLocalGetTime() {
       return handleLong((Long)(TransactionsStatisticsRegistry.getAttribute((IspnStats.LOCAL_GET_EXECUTION))));
   }

   @ManagedAttribute(description = "Average TCB time (in microseconds)")
   @Metric(displayName = "Average TCB time")
   public long getAvgTCBTime() {
      return handleLong((Long)(TransactionsStatisticsRegistry.getAttribute((IspnStats.TBC))));
   }

   @ManagedAttribute(description = "Average NTCB time (in microseconds)")
   @Metric(displayName = "Average NTCB time")
   public long getAvgNTCBTime() {
      return handleLong((Long)(TransactionsStatisticsRegistry.getAttribute((IspnStats.NTBC))));
   }

   @ManagedAttribute(description = "Number of nodes in the cluster")
   @Metric(displayName = "Number of nodes")
   public long getNumNodes() {
      return rpcManagerWrapper.getTransport().getMembers().size();
   }

   @ManagedAttribute(description = "Number of replicas for each key")
   @Metric(displayName = "Replication Degree")
   public long getReplicationDegree(){
      if(this.rpcManagerWrapper != null){
          if(this.rpcManagerWrapper.getTransport() != null){
              if(this.rpcManagerWrapper.getTransport().getMembers() != null){
                   return this.rpcManagerWrapper.getTransport().getMembers().size();
              }
          }
      }

      return 1;
   }

   @ManagedAttribute(description = "Number of concurrent transactions executing on the current node")
   @Metric(displayName = "Local Active Transactions")
   public long getLocalActiveTransactions() {
      if(transactionTable != null){
          return transactionTable.getLocalTxCount();
      }

      return 0;
   }

   @ManagedAttribute(description = "Average Response Time")
   @Metric(displayName = "Average Response Time")
   public long getAvgResponseTime() {

       return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(IspnStats.RESPONSE_TIME));
   }

   //JMX with Transactional class xxxxxxxxx

   @ManagedOperation(description = "Average number of puts performed by a successful local transaction per class")
   @Operation(displayName = "Number of puts per class")
   public long getAvgNumPutsBySuccessfulLocalTxParam(String transactionalClass){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.PUTS_PER_LOCAL_TX,transactionalClass));
   }

   @ManagedOperation(description = "Average Prepare Round-Trip Time duration (in microseconds) per class")
   @Operation(displayName = "Average Prepare RTT per class")
   public long getAvgPrepareRttParam(String transactionalClass) {
      return handleLong((Long)(TransactionsStatisticsRegistry.getAttribute((IspnStats.RTT_PREPARE),transactionalClass)));
   }

   @ManagedOperation(description = "Average Commit Round-Trip Time duration (in microseconds) per class")
   @Operation(displayName = "Average Commit RTT per class")
   public long getAvgCommitRttParam(String transactionalClass) {
      return handleLong((Long)(TransactionsStatisticsRegistry.getAttribute((IspnStats.RTT_COMMIT),transactionalClass)));
   }

   @ManagedOperation(description = "Average Remote Get Round-Trip Time duration (in microseconds) per class")
   @Operation(displayName = "Average Remote Get RTT per class")
   public long getAvgRemoteGetRttParam(String transactionalClass) {
      return handleLong((Long)(TransactionsStatisticsRegistry.getAttribute((IspnStats.RTT_GET),transactionalClass)));
   }

   @ManagedOperation(description = "Average Rollback Round-Trip Time duration (in microseconds) per class")
   @Operation(displayName = "Average Rollback RTT per class")
   public long getAvgRollbackRttParam(String transactionalClass) {
      return handleLong((Long)(TransactionsStatisticsRegistry.getAttribute((IspnStats.RTT_ROLLBACK),transactionalClass)));
   }

   @ManagedOperation(description = "Average asynchronous Prepare duration (in microseconds) per class")
   @Operation(displayName = "Average Prepare Async per class")
   public long getAvgPrepareAsyncParam(String transactionalClass) {
      return handleLong((Long)(TransactionsStatisticsRegistry.getAttribute((IspnStats.ASYNC_PREPARE),transactionalClass)));
   }

   @ManagedOperation(description = "Average asynchronous Commit duration (in microseconds) per class")
   @Operation(displayName = "Average Commit Async per class")
   public long getAvgCommitAsyncParam(String transactionalClass) {
      return handleLong((Long)(TransactionsStatisticsRegistry.getAttribute((IspnStats.ASYNC_COMMIT),transactionalClass)));
   }

   @ManagedOperation(description = "Average asynchronous Complete Notification duration (in microseconds) per class")
   @Operation(displayName = "Average Complete Notification Async per class")
   public long getAvgCompleteNotificationAsyncParam(String transactionalClass) {
      return handleLong((Long)(TransactionsStatisticsRegistry.getAttribute((IspnStats.ASYNC_COMPLETE_NOTIFY),transactionalClass)));
   }

   @ManagedOperation(description = "Average asynchronous Rollback duration (in microseconds) per class")
   @Operation(displayName = "Average Rollback Async per class")
   public long getAvgRollbackAsyncParam(String transactionalClass) {
      return handleLong((Long)(TransactionsStatisticsRegistry.getAttribute((IspnStats.ASYNC_ROLLBACK),transactionalClass)));
   }

   @ManagedOperation(description = "Average number of nodes in Commit destination set per class")
   @Operation(displayName = "Average Number of Nodes in Commit Destination Set per class")
   public long getAvgNumNodesCommitParam(String transactionalClass) {
      return handleLong((Long)(TransactionsStatisticsRegistry.getAttribute((IspnStats.NUM_NODES_COMMIT),transactionalClass)));
   }

   @ManagedOperation(description = "Average number of nodes in Complete Notification destination set per class")
   @Operation(displayName = "Average Number of Nodes in Complete Notification Destination Set per class")
   public long getAvgNumNodesCompleteNotificationParam(String transactionalClass) {
      return handleLong((Long)(TransactionsStatisticsRegistry.getAttribute((IspnStats.NUM_NODES_COMPLETE_NOTIFY),transactionalClass)));
   }

   @ManagedOperation(description = "Average number of nodes in Remote Get destination set per class")
   @Operation(displayName = "Average Number of Nodes in Remote Get Destination Set per class")
   public long getAvgNumNodesRemoteGetParam(String transactionalClass) {
      return handleLong((Long)(TransactionsStatisticsRegistry.getAttribute((IspnStats.NUM_NODES_GET),transactionalClass)));
   }

   @ManagedOperation(description = "Average number of nodes in Prepare destination set per class")
   @Operation(displayName = "Average Number of Nodes in Prepare Destination Set per class")
   public long getAvgNumNodesPrepareParam(String transactionalClass) {
      return handleLong((Long)(TransactionsStatisticsRegistry.getAttribute((IspnStats.NUM_NODES_PREPARE),transactionalClass)));
   }

   @ManagedOperation(description = "Average number of nodes in Rollback destination set per class")
   @Operation(displayName = "Average Number of Nodes in Rollback Destination Set per class")
   public long getAvgNumNodesRollbackParam(String transactionalClass) {
      return handleLong((Long)(TransactionsStatisticsRegistry.getAttribute((IspnStats.NUM_NODES_ROLLBACK),transactionalClass)));
   }

   @ManagedOperation(description = "Application Contention Factor per class")
   @Operation(displayName = "Application Contention Factor per class")
   public double getApplicationContentionFactorParam(String transactionalClass) {
      return handleDouble((Double)TransactionsStatisticsRegistry.getAttribute((IspnStats.APPLICATION_CONTENTION_FACTOR),transactionalClass));
   }

   @Deprecated
   @ManagedOperation(description = "Local Contention Probability per class")
   @Operation(displayName = "Local Conflict Probability per class")
   public double getLocalContentionProbabilityParam(String transactionalClass){
      return handleDouble((Double)TransactionsStatisticsRegistry.getAttribute((IspnStats.LOCAL_CONTENTION_PROBABILITY),transactionalClass));
   }

   @ManagedOperation(description = "Remote Contention Probability per class")
   @Operation(displayName = "Remote Conflict Probability per class")
   public double getRemoteContentionProbabilityParam(String transactionalClass){
      return handleDouble((Double)TransactionsStatisticsRegistry.getAttribute((IspnStats.REMOTE_CONTENTION_PROBABILITY),transactionalClass));
   }

   @ManagedOperation(description = "Lock Contention Probability per class")
   @Operation(displayName = "Lock Contention Probability per class")
   public double getLockContentionProbabilityParam(String transactionalClass){
      return handleDouble((Double)TransactionsStatisticsRegistry.getAttribute((IspnStats.LOCK_CONTENTION_PROBABILITY),transactionalClass));
   }

   @ManagedOperation(description = "Local execution time of a transaction without the time waiting for lock acquisition per class")
   @Operation(displayName = "Local Execution Time Without Locking Time per class")
   public long getLocalExecutionTimeWithoutLockParam(String transactionalClass){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.LOCAL_EXEC_NO_CONT,transactionalClass));
   }

   @ManagedOperation(description = "Average lock holding time (in microseconds) per class")
   @Operation(displayName = "Average Lock Holding Time per class")
   public long getAvgLockHoldTimeParam(String transactionalClass){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.LOCK_HOLD_TIME,transactionalClass));
   }

   @ManagedOperation(description = "Average lock local holding time (in microseconds) per class")
   @Operation(displayName = "Average Lock Local Holding Time per class")
   public long getAvgLocalLockHoldTimeParam(String transactionalClass){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.LOCK_HOLD_TIME_LOCAL,transactionalClass));
   }

   @ManagedOperation(description = "Average lock remote holding time (in microseconds) per class")
   @Operation(displayName = "Average Lock Remote Holding Time per class")
   public long getAvgRemoteLockHoldTimeParam(String transactionalClass){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.LOCK_HOLD_TIME_REMOTE,transactionalClass));
   }

   @ManagedOperation(description = "Average local commit duration time (2nd phase only) (in microseconds) per class")
   @Operation(displayName = "Average Commit Time per class")
   public long getAvgCommitTimeParam(String transactionalClass){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.COMMIT_EXECUTION_TIME,transactionalClass));
   }

   @ManagedOperation(description = "Average local rollback duration time (2nd phase only) (in microseconds) per class")
   @Operation(displayName = "Average Rollback Time per class")
   public long getAvgRollbackTimeParam(String transactionalClass){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.ROLLBACK_EXECUTION_TIME,transactionalClass));
   }

   @ManagedOperation(description = "Average prepare command size (in bytes) per class")
   @Operation(displayName = "Average Prepare Command Size per class")
   public long getAvgPrepareCommandSizeParam(String transactionalClass){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.PREPARE_COMMAND_SIZE,transactionalClass));
   }

   @ManagedOperation(description = "Average commit command size (in bytes) per class")
   @Operation(displayName = "Average Commit Command Size per class")
   public long getAvgCommitCommandSizeParam(String transactionalClass){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.COMMIT_COMMAND_SIZE,transactionalClass));
   }

   @ManagedOperation(description = "Average clustered get command size (in bytes) per class")
   @Operation(displayName = "Average Clustered Get Command Size per class")
   public long getAvgClusteredGetCommandSizeParam(String transactionalClass){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.CLUSTERED_GET_COMMAND_SIZE,transactionalClass));
   }

   @ManagedOperation(description = "Average time waiting for the lock acquisition (in microseconds) per class")
   @Operation(displayName = "Average Lock Waiting Time per class")
   public long getAvgLockWaitingTimeParam(String transactionalClass){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.LOCK_WAITING_TIME,transactionalClass));
   }

   @ManagedOperation(description = "Average transaction arrival rate, originated locally and remotely (in transaction " +
           "per second) per class")
   @Operation(displayName = "Average Transaction Arrival Rate per class")
   public double getAvgTxArrivalRateParam(String transactionalClass){
      return handleDouble((Double)TransactionsStatisticsRegistry.getAttribute(IspnStats.ARRIVAL_RATE,transactionalClass));
   }

   @ManagedOperation(description = "Percentage of Write transaction executed locally (committed and aborted) per class")
   @Operation(displayName = "Percentage of Write Transactions per class")
   public double getPercentageWriteTransactionsParam(String transactionalClass){
      return handleDouble((Double)TransactionsStatisticsRegistry.getAttribute(IspnStats.TX_WRITE_PERCENTAGE,transactionalClass));
   }

   @ManagedOperation(description = "Percentage of Write transaction executed in all successfully executed " +
           "transactions (local transaction only) per class")
   @Operation(displayName = "Percentage of Successfully Write Transactions per class")
   public double getPercentageSuccessWriteTransactionsParam(String transactionalClass){
      return handleDouble((Double)TransactionsStatisticsRegistry.getAttribute(IspnStats.SUCCESSFUL_WRITE_PERCENTAGE,transactionalClass));
   }

   @ManagedOperation(description = "The number of aborted transactions due to timeout in lock acquisition per class")
   @Operation(displayName = "Number of Aborted Transaction due to Lock Acquisition Timeout per class")
   public long getNumAbortedTxDueTimeoutParam(String transactionalClass){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_LOCK_FAILED_TIMEOUT,transactionalClass));
   }

   @ManagedOperation(description = "The number of aborted transactions due to deadlock per class")
   @Operation(displayName = "Number of Aborted Transaction due to Deadlock per class")
   public long getNumAbortedTxDueDeadlockParam(String transactionalClass){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_LOCK_FAILED_DEADLOCK,transactionalClass));
   }

   @ManagedOperation(description = "Average successful read-only transaction duration (in microseconds) per class")
   @Operation(displayName = "Average Read-Only Transaction Duration per class")
   public long getAvgReadOnlyTxDurationParam(String transactionalClass){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.RO_TX_SUCCESSFUL_EXECUTION_TIME,transactionalClass));
   }

   @ManagedOperation(description = "Average successful write transaction duration (in microseconds) per class")
   @Operation(displayName = "Average Write Transaction Duration per class")
   public long getAvgWriteTxDurationParam(String transactionalClass){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.WR_TX_SUCCESSFUL_EXECUTION_TIME,transactionalClass));
   }

   @ManagedOperation(description = "Average write transaction local execution time (in microseconds) per class")
   @Operation(displayName = "Average Write Transaction Local Execution Time per class")
   public long getAvgWriteTxLocalExecutionParam(String transactionalClass){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.WR_TX_LOCAL_EXECUTION_TIME,transactionalClass));
   }

   @ManagedOperation(description = "Average number of locks per write local transaction per class")
   @Operation(displayName = "Average Number of Lock per Local Transaction per class")
   public long getAvgNumOfLockLocalTxParam(String transactionalClass){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_LOCK_PER_LOCAL_TX,transactionalClass));
   }

   @ManagedOperation(description = "Average number of locks per write remote transaction per class")
   @Operation(displayName = "Average Number of Lock per Remote Transaction per class")
   public long getAvgNumOfLockRemoteTxParam(String transactionalClass){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_LOCK_PER_REMOTE_TX,transactionalClass));
   }

   @ManagedOperation(description = "Average number of locks per successfully write local transaction per class")
   @Operation(displayName = "Average Number of Lock per Successfully Local Transaction per class")
   public long getAvgNumOfLockSuccessLocalTxParam(String transactionalClass){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_LOCK_PER_SUCCESS_LOCAL_TX,transactionalClass));
   }

   @ManagedOperation(description = "Average time it takes to execute the prepare command locally (in microseconds) per class")
   @Operation(displayName = "Average Local Prepare Execution Time per class")
   public long getAvgLocalPrepareTimeParam(String transactionalClass){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.LOCAL_PREPARE_EXECUTION_TIME,transactionalClass));
   }

   @ManagedOperation(description = "Average time it takes to execute the prepare command remotely (in microseconds) per class")
   @Operation(displayName = "Average Remote Prepare Execution Time per class")
   public long getAvgRemotePrepareTimeParam(String transactionalClass){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.REMOTE_PREPARE_EXECUTION_TIME,transactionalClass));
   }

   @ManagedOperation(description = "Average time it takes to execute the commit command locally (in microseconds) per class")
   @Operation(displayName = "Average Local Commit Execution Time per class")
   public long getAvgLocalCommitTimeParam(String transactionalClass){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.LOCAL_COMMIT_EXECUTION_TIME,transactionalClass));
   }

   @ManagedOperation(description = "Average time it takes to execute the commit command remotely (in microseconds) per class")
   @Operation(displayName = "Average Remote Commit Execution Time per class")
   public long getAvgRemoteCommitTimeParam(String transactionalClass){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.REMOTE_COMMIT_EXECUTION_TIME,transactionalClass));
   }

   @ManagedOperation(description = "Average time it takes to execute the rollback command locally (in microseconds) per class")
   @Operation(displayName = "Average Local Rollback Execution Time per class")
   public long getAvgLocalRollbackTimeParam(String transactionalClass){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.LOCAL_ROLLBACK_EXECUTION_TIME,transactionalClass));
   }

   @ManagedOperation(description = "Average time it takes to execute the rollback command remotely (in microseconds) per class")
   @Operation(displayName = "Average Remote Rollback Execution Time per class")
   public long getAvgRemoteRollbackTimeParam(String transactionalClass){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.REMOTE_ROLLBACK_EXECUTION_TIME,transactionalClass));
   }

   @ManagedOperation(description = "Average time it takes to execute the rollback command remotely (in microseconds) per class")
   @Operation(displayName = "Average Remote Transaction Completion Notify Execution Time per class")
   public long getAvgRemoteTxCompleteNotifyTimeParam(String transactionalClass){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.TX_COMPLETE_NOTIFY_EXECUTION_TIME,transactionalClass));
   }

   @ManagedOperation(description = "Abort Rate per class")
   @Operation(displayName = "Abort Rate per class")
   public double getAbortRateParam(String transactionalClass){
      return handleDouble((Double)TransactionsStatisticsRegistry.getAttribute(IspnStats.ABORT_RATE,transactionalClass));
   }

   @ManagedOperation(description = "Throughput (in transactions per second) per class")
   @Operation(displayName = "Throughput per class")
   public double getThroughputParam(String transactionalClass){
      return handleDouble((Double)TransactionsStatisticsRegistry.getAttribute(IspnStats.THROUGHPUT,transactionalClass));
   }

   @ManagedOperation(description = "Average number of get operations per (local) read-only transaction per class")
   @Operation(displayName = "Average number of get operations per (local) read-only transaction per class")
   public long getAvgGetsPerROTransactionParam(String transactionalClass){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_SUCCESSFUL_GETS_RO_TX,transactionalClass));
   }

   @ManagedOperation(description = "Average number of get operations per (local) read-write transaction per class")
   @Operation(displayName = "Average number of get operations per (local) read-write transaction per class")
   public long getAvgGetsPerWrTransactionParam(String transactionalClass){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_SUCCESSFUL_GETS_WR_TX,transactionalClass));
   }

   @ManagedOperation(description = "Average number of remote get operations per (local) read-write transaction per class")
   @Operation(displayName = "Average number of remote get operations per (local) read-write transaction per class")
   public long getAvgRemoteGetsPerWrTransactionParam(String transactionalClass){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_SUCCESSFUL_REMOTE_GETS_WR_TX,transactionalClass));
   }

   @ManagedOperation(description = "Average number of remote get operations per (local) read-only transaction per class")
   @Operation(displayName = "Average number of remote get operations per (local) read-only transaction per class")
   public long getAvgRemoteGetsPerROTransactionParam(String transactionalClass){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_SUCCESSFUL_REMOTE_GETS_RO_TX,transactionalClass));
   }

   @ManagedOperation(description = "Average cost of a remote get per class")
   @Operation(displayName = "Remote get cost per class")
   public long getRemoteGetExecutionTimeParam(String transactionalClass){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.REMOTE_GET_EXECUTION,transactionalClass));
   }

   @ManagedOperation(description = "Average number of put operations per (local) read-write transaction per class")
   @Operation(displayName = "Average number of put operations per (local) read-write transaction per class")
   public long getAvgPutsPerWrTransactionParam(String transactionalClass){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_SUCCESSFUL_PUTS_WR_TX,transactionalClass));
   }

   @ManagedOperation(description = "Average number of remote put operations per (local) read-write transaction per class")
   @Operation(displayName = "Average number of remote put operations per (local) read-write transaction per class")
   public long getAvgRemotePutsPerWrTransactionParam(String transactionalClass){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_SUCCESSFUL_REMOTE_PUTS_WR_TX,transactionalClass));
   }

   @ManagedOperation(description = "Average cost of a remote put per class")
   @Operation(displayName = "Remote put cost per class")
   public long getRemotePutExecutionTimeParam(String transactionalClass){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.REMOTE_PUT_EXECUTION,transactionalClass));
   }

   @ManagedOperation(description = "Number of gets performed since last reset per class")
   @Operation(displayName = "Number of Gets per class")
   public long getNumberOfGetsParam(String transactionalClass){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_GET,transactionalClass));
   }

   @ManagedOperation(description = "Number of remote gets performed since last reset per class")
   @Operation(displayName = "Number of Remote Gets per class")
   public long getNumberOfRemoteGetsParam(String transactionalClass){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_REMOTE_GET,transactionalClass));
   }

   @ManagedOperation(description = "Number of puts performed since last reset per class")
   @Operation(displayName = "Number of Puts per class")
   public long getNumberOfPutsParam(String transactionalClass){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_PUT,transactionalClass));
   }

   @ManagedOperation(description = "Number of remote puts performed since last reset per class")
   @Operation(displayName = "Number of Remote Puts per class")
   public long getNumberOfRemotePutsParam(String transactionalClass){
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_REMOTE_PUT,transactionalClass));
   }

   @ManagedOperation(description = "Number of committed transactions since last reset per class")
   @Operation(displayName = "Number Of Commits per class")
   public long getNumberOfCommitsParam(String transactionalClass) {
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_COMMITS,transactionalClass));
   }

   @ManagedOperation(description = "Number of local committed transactions since last reset per class")
   @Operation(displayName = "Number Of Local Commits per class")
   public long getNumberOfLocalCommitsParam(String transactionalClass) {
      return handleLong((Long)TransactionsStatisticsRegistry.getAttribute(IspnStats.NUM_LOCAL_COMMITS,transactionalClass));
   }

   @ManagedOperation(description = "Write skew probability per class")
   @Operation(displayName = "Write Skew Probability per class")
   public double getWriteSkewProbabilityParam(String transactionalClass) {
      return handleDouble((Double)TransactionsStatisticsRegistry.getAttribute(IspnStats.WRITE_SKEW_PROBABILITY,transactionalClass));
   }

   @ManagedOperation(description = "K-th percentile of local read-only transactions execution time per class")
   @Operation(displayName = "K-th Percentile Local Read-Only Transactions per class")
   public double getPercentileLocalReadOnlyTransactionParam(int percentile,String transactionalClass){
      return handleDouble((Double)TransactionsStatisticsRegistry.getPercentile(IspnStats.RO_LOCAL_PERCENTILE, percentile,transactionalClass));
   }

   @ManagedOperation(description = "K-th percentile of remote read-only transactions execution time per class")
   @Operation(displayName = "K-th Percentile Remote Read-Only Transactions per class")
   public double getPercentileRemoteReadOnlyTransactionParam(int percentile,String transactionalClass){
      return handleDouble((Double)TransactionsStatisticsRegistry.getPercentile(IspnStats.RO_REMOTE_PERCENTILE, percentile,transactionalClass));
   }

   @ManagedOperation(description = "K-th percentile of local write transactions execution time per class")
   @Operation(displayName = "K-th Percentile Local Write Transactions per class")
   public double getPercentileLocalRWriteTransactionParam(int percentile,String transactionalClass){
      return handleDouble((Double)TransactionsStatisticsRegistry.getPercentile(IspnStats.WR_LOCAL_PERCENTILE, percentile,transactionalClass));
   }

   @ManagedOperation(description = "K-th percentile of remote write transactions execution time per class")
   @Operation(displayName = "K-th Percentile Remote Write Transactions per class")
   public double getPercentileRemoteWriteTransactionParam(int percentile,String transactionalClass){
      return handleDouble((Double)TransactionsStatisticsRegistry.getPercentile(IspnStats.WR_REMOTE_PERCENTILE, percentile,transactionalClass));
   }

   @ManagedOperation(description = "Average Local processing Get time (in microseconds) per class")
   @Operation(displayName = "Average Local Get time per class")
   public long getAvgLocalGetTimeParam(String transactionalClass) {
      return handleLong((Long)(TransactionsStatisticsRegistry.getAttribute((IspnStats.LOCAL_GET_EXECUTION),transactionalClass)));
   }

   @ManagedOperation(description = "Average TCB time (in microseconds) per class")
   @Operation(displayName = "Average TCB time per class")
   public long getAvgTCBTimeParam(String transactionalClass) {
      return handleLong((Long)(TransactionsStatisticsRegistry.getAttribute((IspnStats.TBC),transactionalClass)));
   }

   @ManagedOperation(description = "Average NTCB time (in microseconds) per class")
   @Operation(displayName = "Average NTCB time per class")
   public long getAvgNTCBTimeParam(String transactionalClass) {
      return handleLong((Long)(TransactionsStatisticsRegistry.getAttribute((IspnStats.NTBC),transactionalClass)));
   }
}
