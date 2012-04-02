package org.infinispan.stats;

import org.infinispan.stats.translations.ExposedStatistics;

/**
 * Websiste: www.cloudtm.eu
 * Date: 20/04/12
 * @author Diego Didona <didona@gsd.inesc-id.pt>
 * @since 5.2
 */
public interface InfinispanStat {

   long getValue(ExposedStatistics.IspnStats param);
   void addValue(ExposedStatistics.IspnStats param, double delta);
   void incrementValue(ExposedStatistics.IspnStats param);

}
