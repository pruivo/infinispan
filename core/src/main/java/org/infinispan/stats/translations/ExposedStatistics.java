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
package org.infinispan.stats.translations;

/**
 * Date: 28/12/11
 * Time: 15:38
 * @author Diego Didona <didona@gsd.inesc-id.pt>
 * @author Pedro Ruivo
 * @since 5.2
 */

public class ExposedStatistics {

   public static enum IspnStats {
      LOCK_WAITING_TIME(true, true),         // C
      LOCK_HOLD_TIME(true, true),            // C
      NUM_HELD_LOCKS(true, true),            // C
      NUM_HELD_LOCKS_SUCCESS_TX(true, false),   // L
      WR_TX_LOCAL_EXECUTION_TIME(true, false),  // L

      NUM_COMMITTED_RO_TX(true, true), // C
      NUM_COMMITTED_WR_TX(true, true), // C
      NUM_ABORTED_WR_TX(true, true),   // C
      NUM_ABORTED_RO_TX(true, true),   // C
      NUM_COMMITS(false, false),  //ONLY FOR QUERY
      NUM_LOCAL_COMMITS(false, false),  //ONLY FOR QUERY

      NUM_PREPARES(true, false), // L      
      LOCAL_EXEC_NO_CONT(true, false),            // L
      LOCAL_CONTENTION_PROBABILITY(false, false),  // ONLY FOR QUERY, derived on the fly
      REMOTE_CONTENTION_PROBABILITY(false, false), //ONLY FOR QUERY, derived on the fly
      LOCK_CONTENTION_PROBABILITY(false,false), //ONLY FOR QUERY, derived on the fly
      LOCK_HOLD_TIME_LOCAL(false,false), //ONLY FOR QUERY
      LOCK_HOLD_TIME_REMOTE(false,false), //ONLY FOR QUERY
      LOCK_CONTENTION_TO_LOCAL(true, true),  // C
      LOCK_CONTENTION_TO_REMOTE(true, true), // C
      NUM_SUCCESSFUL_PUTS(true, false),   // L, this includes also repeated puts over the same item
      PUTS_PER_LOCAL_TX(false, false), // ONLY FOR QUERY, derived on the fly
      NUM_WAITED_FOR_LOCKS(true, true),   // C      
      NUM_REMOTE_GET(true, true),                  // C
      NUM_GET(true,true),                          // C
      NUM_SUCCESSFUL_GETS_RO_TX(true,true),        // C
      NUM_SUCCESSFUL_GETS_WR_TX(true,true),        // C
      NUM_SUCCESSFUL_REMOTE_GETS_WR_TX(true,true), // C
      NUM_SUCCESSFUL_REMOTE_GETS_RO_TX(true,true), // C
      LOCAL_GET_EXECUTION(true, true),
      ALL_GET_EXECUTION(true,true),
      REMOTE_GET_EXECUTION(true, true),            // C
      REMOTE_PUT_EXECUTION(true, true),            // C
      NUM_REMOTE_PUT(true, true),                  // C
      NUM_PUT(true, true),                         // C      
      NUM_SUCCESSFUL_PUTS_WR_TX(true,true),        // C
      NUM_SUCCESSFUL_REMOTE_PUTS_WR_TX(true,true), // C
      TX_WRITE_PERCENTAGE(false, false),           // ONLY FOR QUERY, derived on the fly
      SUCCESSFUL_WRITE_PERCENTAGE(false, false),   // ONLY FOR QUERY, derived on the fly
      WR_TX_ABORTED_EXECUTION_TIME(true, true),    //C
      WR_TX_SUCCESSFUL_EXECUTION_TIME(true, true), //C
      RO_TX_SUCCESSFUL_EXECUTION_TIME(true, true), //C
      RO_TX_ABORTED_EXECUTION_TIME(true, true),    //C
      APPLICATION_CONTENTION_FACTOR(false, false), // ONLY FOR QUERY

      NUM_WRITE_SKEW(true, false), // L
      WRITE_SKEW_PROBABILITY(false, false), // ONLY FOR QUERY

      //Abort rate, arrival rate and throughput
      ABORT_RATE(false, false),     // ONLY FOR QUERY, derived on the fly
      ARRIVAL_RATE(false, false),   // ONLY FOR QUERY, derived on the fly
      THROUGHPUT(false, false),     // ONLY FOR QUERY, derived on the fly

      //Percentile stuff
      RO_LOCAL_PERCENTILE(false, false),  // ONLY FOR QUERY, derived on the fly
      WR_LOCAL_PERCENTILE(false, false),  // ONLY FOR QUERY, derived on the fly
      RO_REMOTE_PERCENTILE(false, false), // ONLY FOR QUERY, derived on the fly
      WR_REMOTE_PERCENTILE(false, false), // ONLY FOR QUERY, derived on the fly

      //Prepare, rollback and commit execution times
      ROLLBACK_EXECUTION_TIME(true, true),   // C
      NUM_ROLLBACKS(true, true),             // C
      LOCAL_ROLLBACK_EXECUTION_TIME(false, false),    // ONLY FOR QUERY, derived on the fly
      REMOTE_ROLLBACK_EXECUTION_TIME(false, false),   // ONLY FOR QUERY, derived on the fly

      COMMIT_EXECUTION_TIME(true, true),     // C
      NUM_COMMIT_COMMAND(true, true),        // C
      LOCAL_COMMIT_EXECUTION_TIME(false, false),      // ONLY FOR QUERY, derived on the fly
      REMOTE_COMMIT_EXECUTION_TIME(false, false),     // ONLY FOR QUERY, derived on the fly

      PREPARE_EXECUTION_TIME(true, true),    // C
      NUM_PREPARE_COMMAND(true, true),       // C
      LOCAL_PREPARE_EXECUTION_TIME(false, false),     // ONLY FOR QUERY, derived on the fly
      REMOTE_PREPARE_EXECUTION_TIME(false, false),    // ONLY FOR QUERY, derived on the fly

      TX_COMPLETE_NOTIFY_EXECUTION_TIME(false, true),    // R
      NUM_TX_COMPLETE_NOTIFY_COMMAND(false, true),       // R

      //Lock querying
      NUM_LOCK_PER_LOCAL_TX(false, false),         // ONLY FOR QUERY, derived on the fly
      NUM_LOCK_PER_REMOTE_TX(false, false),        // ONLY FOR QUERY, derived on the fly
      NUM_LOCK_PER_SUCCESS_LOCAL_TX(false, false), // ONLY FOR QUERY, derived on the fly

      //commands size
      PREPARE_COMMAND_SIZE(true, false),        // L
      COMMIT_COMMAND_SIZE(true, false),         // L
      CLUSTERED_GET_COMMAND_SIZE(true, false),  // L

      //Lock failed stuff
      NUM_LOCK_FAILED_TIMEOUT(true, false),  //L
      NUM_LOCK_FAILED_DEADLOCK(true, false), //L

      //RTT STUFF: everything is local && synchronous communication
      NUM_RTTS_PREPARE(true, false),   // L
      RTT_PREPARE(true, false),        // L      
      NUM_RTTS_COMMIT(true, false),    // L
      RTT_COMMIT(true, false),         // L
      NUM_RTTS_ROLLBACK(true, false),  // L
      RTT_ROLLBACK(true, false),       // L
      NUM_RTTS_GET(true, false),       // L
      RTT_GET(true, false),            // L

      //SEND STUFF: everything is local && asynchronous communication
      ASYNC_PREPARE(true, false),               // L
      NUM_ASYNC_PREPARE(true, false),           // L
      ASYNC_COMMIT(true, false),                // L
      NUM_ASYNC_COMMIT(true, false),            // L
      ASYNC_ROLLBACK(true, false),              // L
      NUM_ASYNC_ROLLBACK(true, false),          // L
      ASYNC_COMPLETE_NOTIFY(true, false),       // L
      NUM_ASYNC_COMPLETE_NOTIFY(true, false),   // L            

      //Number of nodes involved stuff
      NUM_NODES_PREPARE(true, false),           //L
      NUM_NODES_COMMIT(true, false),            //L
      NUM_NODES_ROLLBACK(true, false),          //L
      NUM_NODES_COMPLETE_NOTIFY(true, false),   //L
      NUM_NODES_GET(true, false),               //L

      //Additional Stats
      TBC_EXECUTION_TIME(true,false),
      TBC_COUNT(true,false),
      TBC(false,false), //Time between operations in Transaction   // ONLY FOR QUERY, derived on the fly

      NTBC_EXECUTION_TIME(true,false),
      NTBC_COUNT(true,false),
      NTBC(false,false), //Time between Transactions in a thread   // ONLY FOR QUERY, derived on the fly

      RESPONSE_TIME(true,false);

      private boolean local;
      private boolean remote;

      IspnStats(boolean local, boolean remote) {
         this.local = local;
         this.remote = remote;
      }

      public boolean isLocal() {
         return local;
      }

      public boolean isRemote() {
         return remote;
      }
   }
}
