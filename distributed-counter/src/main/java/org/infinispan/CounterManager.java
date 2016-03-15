package org.infinispan;

import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.impl.JGroupsCounter;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.jgroups.Channel;
import org.jgroups.blocks.atomic.CounterService;
import org.jgroups.fork.ForkChannel;
import org.jgroups.protocols.COUNTER;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;

import java.util.Objects;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class CounterManager {

   private static final String COUNTER_FORK_STACK_ID = "counter-manager-fork";
   private static final String COUNTER_FORK_CHANNEL_ID = "counter-manager-fork-ch";
   private static final String COUNTER_CLUSTER_NAME = "counter-manager-cluster";
   private final CounterService counterService;

   private CounterManager(CounterService counterService) {
      this.counterService = counterService;
   }

   public static CounterManager getCounterManager(EmbeddedCacheManager cacheManager) throws Exception {
      Objects.requireNonNull(cacheManager, "EmbeddedCacheManager can't be null.");
      GlobalComponentRegistry registry = cacheManager.getGlobalComponentRegistry();
      CounterManager counterManager = registry.getComponent(CounterManager.class);
      if (counterManager != null) {
         return counterManager;
      }
      Channel channel = createForkChannel(cacheManager.getTransport());
      CounterService service = new CounterService(channel);
      channel.connect(COUNTER_CLUSTER_NAME);
      return new CounterManager(service);
   }

   private static Channel createForkChannel(Transport transport) throws Exception {
      if (transport == null || !(transport instanceof JGroupsTransport)) {
         throw new IllegalArgumentException("Can't create a CounterManager based on the current cache manager. JGroupsTransport is needed.");
      }
      Channel channel = ((JGroupsTransport) transport).getChannel();
      Protocol firstProtocol = channel.getProtocolStack().getTopProtocol();
      return new ForkChannel(channel, COUNTER_FORK_STACK_ID, COUNTER_FORK_CHANNEL_ID, true, ProtocolStack.ABOVE, firstProtocol.getClass(), new COUNTER());
   }

   public Counter getOrCreateCounter(String counterName, int initialValue) {
      return new JGroupsCounter(counterService, counterName, initialValue);
   }

}
