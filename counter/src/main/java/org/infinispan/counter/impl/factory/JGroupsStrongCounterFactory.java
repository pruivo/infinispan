package org.infinispan.counter.impl.factory;

import static org.infinispan.counter.impl.CounterModuleLifecycle.JGROUPS_COUNTER_CHANNEL_ID;

import java.util.concurrent.CompletionStage;

import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterType;
import org.infinispan.counter.impl.jgroups.JGroupsBoundedStrongCounter;
import org.infinispan.counter.impl.jgroups.JGroupsUnboundedStrongCounter;
import org.infinispan.counter.impl.manager.InternalCounterAdmin;
import org.infinispan.counter.logging.Log;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.util.logging.LogFactory;
import org.jgroups.JChannel;
import org.jgroups.blocks.atomic.CounterService;
import org.jgroups.fork.ForkChannel;
import org.jgroups.protocols.COUNTER;
import org.jgroups.util.CompletableFutures;
import org.jgroups.util.Util;

/**
 * TODO! document this
 */
@Scope(Scopes.GLOBAL)
public class JGroupsStrongCounterFactory implements StrongCounterFactory {

   private static final Log log = LogFactory.getLog(JGroupsStrongCounterFactory.class, Log.class);

   @Inject Transport transport;
   @Inject GlobalConfiguration configuration;
   private ForkChannel channel;
   private CounterService counterService;

   @Start
   void start() throws Exception {
      assert transport instanceof JGroupsTransport;
      JChannel mainChannel = ((JGroupsTransport) transport).getChannel();
      if (JGroupsTransport.findFork(mainChannel) == null) {
         throw log.forkProtocolRequired();
      }
      // TODO! set backups
      COUNTER prot = new COUNTER();

      channel = new ForkChannel(mainChannel, JGROUPS_COUNTER_CHANNEL_ID, JGROUPS_COUNTER_CHANNEL_ID, prot);
      counterService = new CounterService(channel);
   }

   @Stop
   void stop() {
      if (channel != null) {
         Util.close(channel);
      }
   }

   @Override
   public CompletionStage<Void> removeStrongCounter(String counterName) {
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<InternalCounterAdmin> createStrongCounter(String counterName, CounterConfiguration configuration) {
      assert configuration.type() != CounterType.WEAK;
      return counterService.getOrCreateAsyncCounter(counterName, configuration.initialValue())
            .thenApply(counter -> configuration.type() == CounterType.BOUNDED_STRONG ?
                  new JGroupsBoundedStrongCounter(counterName, configuration, counter) :
                  new JGroupsUnboundedStrongCounter(counterName, configuration, counter));
   }
}
