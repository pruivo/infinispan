package org.infinispan.stats.percentiles;

/**
 * Websiste: www.cloudtm.eu
 * Date: 02/05/12
 * @author Diego Didona <didona@gsd.inesc-id.pt>
 * @since 5.2
 */
public final class PercentileStatsFactory {

   public static PercentileStats createNewPercentileStats(){
      return new ReservoirSampling();
   }
}
