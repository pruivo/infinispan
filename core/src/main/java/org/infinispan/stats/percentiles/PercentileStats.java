package org.infinispan.stats.percentiles;

/**
 * Websiste: www.cloudtm.eu
 * Date: 20/04/12
 * @author Diego Didona <didona@gsd.inesc-id.pt>
 * @since 5.2
 */
public interface PercentileStats {

   double getKPercentile(int percentile);
   void insertSample(double value);
   void reset();
}
