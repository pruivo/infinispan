package org.infinispan.topology;

import org.infinispan.commons.CacheException;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.concurrent.Collector;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;

/**
 * Created with
 *
 * @author Dan Berindei
 * @author Pedro Ruivo
 * @since 5.2
 */
class RebalanceConfirmationCollector {
   private final static Log log = LogFactory.getLog(RebalanceConfirmationCollector.class);
   private final static boolean trace = log.isTraceEnabled();

   private final Collector<Address> collector;
   private final String cacheName;
   private final int topologyId;

   public RebalanceConfirmationCollector(String cacheName, int topologyId, Collection<Address> members) {
      this.collector = new Collector<Address>(members);
      this.cacheName = cacheName;
      this.topologyId = topologyId;
      if (trace) {
         log.tracef("Initialized rebalance confirmation collector %d@%s, initial list is %s",
                    topologyId, cacheName, collector.pending());
      }
   }

   /**
    * @return {@code true} if everyone has confirmed
    */
   public boolean confirmRebalance(Address node, int receivedTopologyId) {
      if (topologyId > receivedTopologyId) {
         throw new CacheException(String.format("Received invalid rebalance confirmation from %s for cache %s, " +
                                                      "expecting topology id %d but got %d", node, cacheName,
                                                topologyId, receivedTopologyId));
      }

      boolean removed = collector.confirm(node);
      if (!removed) {
         if (trace) {
            log.tracef("Rebalance confirmation collector %d@%s ignored confirmation for %s, which is already confirmed",
                       topologyId, cacheName, node);
         }
         return false;
      }

      log.tracef("Rebalance confirmation collector %d@%s received confirmation for %s, remaining list is %s",
                 topologyId, cacheName, node, collector.pending());
      return collector.isAllCollected();
   }


   /**
    * @return {@code true} if everyone has confirmed
    */
   public boolean updateMembers(Collection<Address> newMembers) {
      // only return true the first time
      collector.retain(newMembers);
      log.tracef("Rebalance confirmation collector %d@%s members list updated, remaining list is %s",
                 topologyId, cacheName, collector.pending());
      return collector.isAllCollected();
   }

   @Override
   public String toString() {
      return "RebalanceConfirmationCollector{" +
            "collector=" + collector +
            ", cacheName='" + cacheName + '\'' +
            ", topologyId=" + topologyId +
            '}';
   }
}
