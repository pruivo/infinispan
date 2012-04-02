package org.infinispan.stats;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.stats.translations.ExposedStatistics;
import org.infinispan.stats.translations.RemoteStatistics;

/**
 * Websiste: www.cloudtm.eu
 * Date: 20/04/12
 * @author Diego Didona <didona@gsd.inesc-id.pt>
 * @since 5.2
 */
public class RemoteTransactionStatistics extends TransactionStatistics{

   public RemoteTransactionStatistics(Configuration configuration){
      super(RemoteStatistics.getSize(),configuration);
   }

   protected final void onPrepareCommand(){
      //nop
   }

   @Override
   protected final void terminate() {
      //nop
   }

   protected final int getIndex(ExposedStatistics.IspnStats stat) throws NoIspnStatException{
      int ret = RemoteStatistics.getIndex(stat);
      if (ret == RemoteStatistics.NOT_FOUND) {
         throw new NoIspnStatException("Statistic "+stat+" is not available!");
      }
      return ret;
   }

   @Override
   public final String toString() {
      return "RemoteTransactionStatistics{" + super.toString();
   }
}
