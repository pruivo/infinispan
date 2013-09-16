package org.infinispan.stats.container.transactional;

import org.infinispan.util.TimeService;

/**
 * //TODO: document this!
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
public class RemoteTxStatisticContainer extends BaseTxStatisticsContainer {

   public RemoteTxStatisticContainer(TimeService timeService) {
      super(timeService, -1);
   }
}
