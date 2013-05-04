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
import org.infinispan.stats.translations.LocalStatistics;
import org.infinispan.transaction.LockingMode;

/**
 * Websiste: www.cloudtm.eu Date: 20/04/12
 *
 * @author Diego Didona <didona@gsd.inesc-id.pt>
 * @author Pedro Ruivo
 * @since 5.2
 */
public class LocalTransactionStatistics extends TransactionStatistics {

   private boolean stillLocalExecution;

   public LocalTransactionStatistics(Configuration configuration) {
      super(LocalStatistics.getSize(), configuration);
      this.stillLocalExecution = true;
   }

   public final void terminateLocalExecution() {
      this.stillLocalExecution = false;
      long cpuTime = sampleServiceTime ? threadMXBean.getCurrentThreadCpuTime() : 0;
      long now = System.nanoTime();
      this.endLocalCpuTime = cpuTime;
      this.endLocalTime = now;
      if (!isReadOnly()) {
         incrementValue(IspnStats.NUM_UPDATE_TX_GOT_TO_PREPARE);
         this.addValue(IspnStats.WR_TX_LOCAL_EXECUTION_TIME, System.nanoTime() - this.initTime);
      }
      //RO can never abort :)
      else {
         addValue(IspnStats.READ_ONLY_TX_LOCAL_R, now - this.initTime);
         if (sampleServiceTime) {
            addValue(IspnStats.READ_ONLY_TX_LOCAL_S, cpuTime - this.initCpuTime);
            //I do not update the number of prepares, because no readOnly transaction fails
         }
      }
      this.incrementValue(IspnStats.NUM_PREPARES);
   }


   public final boolean isStillLocalExecution() {
      return this.stillLocalExecution;
   }

   @Override
    /*
    I take local execution times only if the xact commits, otherwise if I have some kind of transactions which remotely abort more frequently,
    they will bias the accuracy of the statistics, just because they are re-run more often!
     */
   protected final void terminate() {
      long cpuTime = sampleServiceTime ? threadMXBean.getCurrentThreadCpuTime() : 0;
      long now = System.nanoTime();
      if (!isCommit())
         return;
      if (!isReadOnly()) {
         //log.fatal("Terminating xact. Going to sample times");
         long numPuts = this.getValue(IspnStats.NUM_PUT);
         this.addValue(IspnStats.NUM_SUCCESSFUL_PUTS, numPuts);
         this.addValue(IspnStats.NUM_HELD_LOCKS_SUCCESS_TX, getValue(IspnStats.NUM_HELD_LOCKS));
         this.addValue(IspnStats.UPDATE_TX_LOCAL_R, this.endLocalTime - this.initTime);
         this.addValue(IspnStats.UPDATE_TX_TOTAL_R, now - this.initTime);
         if (sampleServiceTime) {
            //log.fatal("Sampling service time: current is "+cpuTime+" endOfLocalExec was "+endLocalCpuTime);
            addValue(IspnStats.UPDATE_TX_LOCAL_S, this.endLocalCpuTime - this.initCpuTime);
            addValue(IspnStats.UPDATE_TX_TOTAL_S, cpuTime - this.initCpuTime);
         }
         if (configuration.transaction().lockingMode() == LockingMode.OPTIMISTIC) {
            this.addValue(IspnStats.LOCAL_EXEC_NO_CONT, this.getValue(IspnStats.UPDATE_TX_LOCAL_R));
         } else {
            long localLockAcquisitionTime = getValue(IspnStats.LOCK_WAITING_TIME);
            long totalLocalDuration = this.getValue(IspnStats.UPDATE_TX_LOCAL_R);
            this.addValue(IspnStats.LOCAL_EXEC_NO_CONT, (totalLocalDuration - localLockAcquisitionTime));
         }
      } else {
         addValue(IspnStats.READ_ONLY_TX_TOTAL_R, now - initTime);
         if (sampleServiceTime)
            addValue(IspnStats.READ_ONLY_TX_TOTAL_S, cpuTime - initCpuTime);
      }
   }

   protected final void onPrepareCommand() {
      this.terminateLocalExecution();
   }

   protected final int getIndex(IspnStats stat) throws NoIspnStatException {
      int ret = LocalStatistics.getIndex(stat);
      if (ret == LocalStatistics.NOT_FOUND) {
         throw new NoIspnStatException("IspnStats " + stat + " not found!");
      }
      return ret;
   }


   @Override
   public final String toString() {
      return "LocalTransactionStatistics{" +
            "stillLocalExecution=" + stillLocalExecution +
            ", " + super.toString();
   }
}
