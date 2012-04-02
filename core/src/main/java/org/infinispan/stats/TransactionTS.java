package org.infinispan.stats;

/**
 * User: roberto
 * Date: 14/12/12
 * Time: 15:46
 */

public class TransactionTS {
   private long endLastTxTs;
   private long NTBC_execution_time;
   private long NTBC_count;

   public long getEndLastTxTs() {
      return endLastTxTs;
   }

   public void setEndLastTxTs(long endLastTxTs) {
      this.endLastTxTs = endLastTxTs;
   }

   public long getNTBC_execution_time() {
      return NTBC_execution_time;
   }

   public void addNTBC_execution_time(long NTBC_execution_time) {
      this.NTBC_execution_time += NTBC_execution_time;
   }

   public long getNTBC_count() {
      return NTBC_count;
   }

   public void addNTBC_count() {
      this.NTBC_count++;
   }
}
