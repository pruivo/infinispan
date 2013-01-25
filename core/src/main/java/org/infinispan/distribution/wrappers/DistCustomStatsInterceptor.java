package org.infinispan.distribution.wrappers;

import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.stats.TransactionsStatisticsRegistry;
import org.infinispan.stats.translations.ExposedStatistics;
import org.rhq.helpers.pluginAnnotations.agent.Metric;


/**
 * Websiste: www.cloudtm.eu
 * Date: 02/05/12
 * @author Diego Didona <didona@gsd.inesc-id.pt>
 * @since 5.2
 */
public class DistCustomStatsInterceptor extends CustomStatsInterceptor {

   private DistributionManager distributionManager;

   @Inject
   public void inject(DistributionManager distributionManager) {
      this.distributionManager = distributionManager;
   }

   @Override
   public boolean isRemote(Object key) {
      return !distributionManager.getLocality(key).isLocal();
   }
   
   @ManagedAttribute(description = "Number of replicas for each key")
   @Metric(displayName = "Replication Degree")
   public long getReplicationDegree() {
      if(distributionManager != null){

	     return distributionManager.getReplicationDegree();

      }
      return 1;
   }
}
