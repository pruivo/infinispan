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
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.stats.translations.ExposedStatistics.IspnStats;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Websiste: www.cloudtm.eu
 * Date: 20/04/12
 * @author Diego Didona <didona@gsd.inesc-id.pt>
 * @author Pedro Ruivo
 * @since 5.2
 */
public final class TransactionsStatisticsRegistry {

   private static final Log log = LogFactory.getLog(TransactionsStatisticsRegistry.class);
   
   public static final String DEFAULT_ISPN_CLASS = "DEFAULT_ISPN_CLASS";

   //Now it is unbounded, we can define a MAX_NO_CLASSES
   private static final Map<String, NodeScopeStatisticCollector> transactionalClassesStatsMap
         = new HashMap<String, NodeScopeStatisticCollector>();

   private static final ConcurrentMap<GlobalTransaction, RemoteTransactionStatistics> remoteTransactionStatistics =
         new ConcurrentHashMap<GlobalTransaction, RemoteTransactionStatistics>();

   private static Configuration configuration;

   //Comment for reviewers: do we really need threadLocal? If I have the global id of the transaction, I can
   //retrieve the transactionStatistics
   private static final ThreadLocal<TransactionStatistics> thread = new ThreadLocal<TransactionStatistics>();
   private static ThreadLocal<TransactionTS> lastTransactionTS = new ThreadLocal<TransactionTS>();

   public static void init(Configuration configuration){
      log.info("Initializing transactionalClassesMap");
      TransactionsStatisticsRegistry.configuration = configuration;
      transactionalClassesStatsMap.put(DEFAULT_ISPN_CLASS, new NodeScopeStatisticCollector(configuration));
   }

   public static void addNTBCValue(long currtime) {
      TransactionStatistics txs = thread.get();
      if (txs == null) {
         log.debug("Trying to add NTBC " +
                 " but no transaction is associated to the thread");
         return;
      }
      if(txs.getLastOpTimestamp() != 0){
         txs.addValue(IspnStats.TBC_EXECUTION_TIME, currtime - txs.getLastOpTimestamp());
      }
   }

   public static void setLastOpTimestamp(long currtime) {
      TransactionStatistics txs = thread.get();
      if (txs == null) {
         log.debug("Trying to set Last Op Timestamp "+
                 " but no transaction is associated to the thread");
         return;
      }
      txs.setLastOpTimestamp(currtime);
   }

   public static void addValue(IspnStats param, double value) {
      TransactionStatistics txs = thread.get();
      if (txs == null) {
         if (log.isDebugEnabled()) {
            log.debugf("Trying to add value %s to parameter %s but no transaction is associated to the thread",
                       value, param);
         }
         return;
      }
      txs.addValue(param, value);
   }

   public static void incrementValue(IspnStats param) {
      TransactionStatistics txs = thread.get();
      if (txs == null) {
         if (log.isDebugEnabled()) {
            log.debugf("Trying to increment to parameter %s but no transaction is associated to the thread", param);
         }
         return;
      }
      txs.addValue(param, 1D);
   }

   public static void addValueAndFlushIfNeeded(IspnStats param, double value, boolean local) {
      NodeScopeStatisticCollector nssc = transactionalClassesStatsMap.get(DEFAULT_ISPN_CLASS);
      if (local) {
         nssc.addLocalValue(param, value);
      } else {
         nssc.addRemoteValue(param, value);
      }
   }

   public static void incrementValueAndFlushIfNeeded(IspnStats param, boolean local) {
      NodeScopeStatisticCollector nssc = transactionalClassesStatsMap.get(DEFAULT_ISPN_CLASS);
      if (local) {
         nssc.addLocalValue(param, 1D);
      } else {
         nssc.addRemoteValue(param, 1D);
      }
   }

   public static void onPrepareCommand() {
      //NB: If I want to give up using the InboundInvocationHandler, I can create the remote transaction
      //here, just overriding the handlePrepareCommand
      TransactionStatistics txs = thread.get();
      if (txs == null) {
         if (log.isDebugEnabled()) {
            log.debug("Trying to invoke onPrepareCommand() but no transaction is associated to the thread");
         }
         return;
      }
      txs.onPrepareCommand();
   }

   public static void setTransactionOutcome(boolean commit) {
      TransactionStatistics txs = thread.get();
      if (txs == null) {
         if (log.isDebugEnabled()) {
            log.debugf("Trying to set outcome to %s but no transaction is associated to the thread",
                       commit ? "Commit" : "Rollback");
         }
         return;
      }
      txs.setCommit(commit);
   }

   public static void terminateTransaction() {
      TransactionStatistics txs = thread.get();
      if (txs == null) {
         if (log.isDebugEnabled()) {
            log.debug("Trying to invoke terminate() but no transaction is associated to the thread");
         }
         return;
      }
      txs.terminateTransaction();

      NodeScopeStatisticCollector dest = transactionalClassesStatsMap.get(txs.getTransactionalClass());
      if (dest != null) {
         dest.merge(txs);
      }else{
          log.debug("Statistics not merged for transaction class not found on transactionalClassStatsMap");
      }

      thread.remove();
      TransactionTS lastTS = lastTransactionTS.get();
      if(lastTS != null)
        lastTS.setEndLastTxTs(System.nanoTime());
   }

   public static Object getAttribute(IspnStats param,String className){

      try{
         if (configuration == null) {
            return null;
         }
         if(className == null){
            return transactionalClassesStatsMap.get(DEFAULT_ISPN_CLASS).getAttribute(param);
         }else{
            if(transactionalClassesStatsMap.get(className) != null)
               return transactionalClassesStatsMap.get(className).getAttribute(param);
            else
               return null;
         }
      }
      catch (Throwable e){
         e.printStackTrace();
         return null;
      }
   }

    public static Object getAttribute(IspnStats param){
       try{
          if (configuration == null) {
             return null;
          }
             return transactionalClassesStatsMap.get(DEFAULT_ISPN_CLASS).getAttribute(param);
       }
       catch(Throwable e){
          e.printStackTrace();
          return null;
       }
    }

   public static Object getPercentile(IspnStats param, int percentile, String className){
      if (configuration == null) {
         return null;
      }
      if(className == null){
          return transactionalClassesStatsMap.get(DEFAULT_ISPN_CLASS).getPercentile(param, percentile);
      }else{
         if(transactionalClassesStatsMap.get(className) != null)
            return transactionalClassesStatsMap.get(className).getPercentile(param, percentile);
         else
            return null;
      }
   }
   public static Object getPercentile(IspnStats param, int percentile){
       if (configuration == null) {
           return null;
       }
       return transactionalClassesStatsMap.get(DEFAULT_ISPN_CLASS).getPercentile(param, percentile);
    }

   public static void addTakenLock(Object lock) {
      TransactionStatistics txs = thread.get();
      if (txs == null) {
         if (log.isDebugEnabled()) {
            log.debugf("Trying to add lock [%s] but no transaction is associated to the thread", lock);
         }
         return;
      }
      txs.addTakenLock(lock);
   }


   public static void setUpdateTransaction() {
      TransactionStatistics txs = thread.get();
      if (txs == null) {
         if (log.isDebugEnabled()) {
            log.debug("Trying to invoke setUpdateTransaction() but no transaction is associated to the thread");
         }
         return;
      }
      txs.setUpdateTransaction();
   }

   //This is synchronized because depending on the local/remote nature, a different object is created
   //Now, remote transactionStatistics get initialized at InboundInvocationHandler level
   public static void initTransactionIfNecessary(TxInvocationContext tctx) {
      boolean isLocal = tctx.isOriginLocal();
      if(isLocal)
         initLocalTransaction();

   }

   public static void attachRemoteTransactionStatistic(GlobalTransaction globalTransaction, boolean createIfAbsent) {
      RemoteTransactionStatistics rts = remoteTransactionStatistics.get(globalTransaction);
      if (rts == null && createIfAbsent && configuration != null) {
         if (log.isTraceEnabled()) {
            log.tracef("Create a new remote transaction statistic for transaction %s", globalTransaction.prettyPrint());
         }
         rts = new RemoteTransactionStatistics(configuration);
         remoteTransactionStatistics.put(globalTransaction, rts);
      } else if (configuration == null) {
         if (log.isDebugEnabled()) {
            log.debugf("Trying to create a remote transaction statistics in a not initialized Transaction Statistics Registry");
         }
         return;
      } else {
         if (log.isTraceEnabled()) {
            log.tracef("Using the remote transaction statistic %s for transaction %s", rts, globalTransaction.prettyPrint());
         }
      }
      thread.set(rts);
   }

   public static void detachRemoteTransactionStatistic(GlobalTransaction globalTransaction, boolean finished) {
      if (thread.get() == null) {
         return;
      }
      if (finished) {
         if (log.isTraceEnabled()) {
            log.tracef("Detach remote transaction statistic and finish transaction %s", globalTransaction.prettyPrint());
         }
         terminateTransaction();
         remoteTransactionStatistics.remove(globalTransaction);
      } else {
         if (log.isTraceEnabled()) {
            log.tracef("Detach remote transaction statistic for transaction %s", globalTransaction.prettyPrint());
         }
         thread.remove();
      }
   }

   public static void reset(){
      for (NodeScopeStatisticCollector nsc : transactionalClassesStatsMap.values()) {
         nsc.reset();
      }
   }

   public static boolean hasStatisticCollector() {
      return thread.get() != null;
   }

    public static void setTransactionalClass(String className){
      TransactionStatistics txs = thread.get();
      if (txs == null) {
          log.debug("Trying to invoke setUpdateTransaction() but no transaction is associated to the thread");
          return;
        }
      txs.setTransactionalClass(className);
      registerTransactionalClass(className);
    }

    private static synchronized void registerTransactionalClass(String className) {
      if(transactionalClassesStatsMap.get(className) == null){
         transactionalClassesStatsMap.put(className,new NodeScopeStatisticCollector(configuration));
      }
    }

    private static void initLocalTransaction(){
      //Not overriding the InitialValue method leads me to have "null" at the first invocation of get()
      TransactionStatistics lts = thread.get();
      if (lts == null && configuration != null) {
         if (log.isTraceEnabled()) {
            log.tracef("Init a new local transaction statistics");
         }
         thread.set(new LocalTransactionStatistics(configuration));
         //Here only when transaction starts
         TransactionTS lastTS = lastTransactionTS.get();
         if (lastTS == null) {
            log.tracef("Init a new local transaction statistics for Timestamp");
            lastTransactionTS.set(new TransactionTS());
         }else{
            addValue(IspnStats.NTBC_EXECUTION_TIME, System.nanoTime() - lastTS.getEndLastTxTs());
            incrementValue(IspnStats.NTBC_COUNT);
         }
      } else if (configuration == null ) {
         if (log.isDebugEnabled()) {
            log.debugf("Trying to create a local transaction statistics in a not initialized Transaction Statistics Registry");
         }
      } else {
         if (log.isTraceEnabled()) {
            log.tracef("Local transaction statistic is already initialized: %s", lts);
         }
      }
   }


}
