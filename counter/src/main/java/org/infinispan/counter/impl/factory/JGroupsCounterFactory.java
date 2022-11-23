package org.infinispan.counter.impl.factory;

import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterType;
import org.infinispan.counter.api.Storage;
import org.infinispan.counter.impl.jgroups.UnboundedJGroupsStrongCounter;
import org.infinispan.counter.impl.manager.InternalCounterAdmin;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.jgroups.JChannel;
import org.jgroups.blocks.atomic.CounterService;
import org.jgroups.fork.ForkChannel;
import org.jgroups.protocols.COUNTER;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;

/**
 * //TODO document this!
 *
 * @author Pedro Ruivo
 * @since 15.0
 */
@Scope(Scopes.GLOBAL)
public class JGroupsCounterFactory implements StrongCounterFactory, Function<String, CompletionStage<Void>> {

   private final JChannel channel;
   private final CounterService counterService;

   private JGroupsCounterFactory(JChannel channel, CounterService counterService) {
      // available counters
      this.channel = channel;
      this.counterService = counterService;
   }

   public static JGroupsCounterFactory create(JGroupsTransport transport, int numOwners) {
      try {
         ForkChannel channel = createAndConnectForkChannel(transport, numOwners);
         return new JGroupsCounterFactory(channel, new CounterService(channel));
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   private static ForkChannel createAndConnectForkChannel(JGroupsTransport transport, int numOwners) throws Exception {
      assert numOwners > 1;
      //TODO this is kind of hacking/bad... we are breaking the Transport abstraction :(
      String stackId = "org.infinispan.COUNTER";
      JChannel channel = transport.getChannel();
      Protocol top = channel.getProtocolStack().getTopProtocol();
      ForkChannel forkChannel = new ForkChannel(channel, stackId, stackId, true, ProtocolStack.Position.ABOVE,
            top.getClass(), new COUNTER().setNumberOfBackups(numOwners - 1));
      forkChannel.connect(stackId);
      return forkChannel;
   }

   @Stop
   public void stop() {
      channel.disconnect();
   }

   @Override
   public CompletionStage<Void> removeStrongCounter(String counterName) {
      counterService.deleteCounter(counterName);
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<InternalCounterAdmin> createStrongCounter(String counterName, CounterConfiguration configuration) {
      assert configuration.type() != CounterType.WEAK;
      if (configuration.type() == CounterType.BOUNDED_STRONG) {
         throw new UnsupportedOperationException();
      }
      if (configuration.lifespan() > 0) {
         throw new IllegalArgumentException();
      }
      if (configuration.storage() == Storage.PERSISTENT) {
         throw new IllegalArgumentException();
      }
      return counterService.getOrCreateAsyncCounter(counterName, configuration.initialValue())
            .thenApply(asyncCounter -> new UnboundedJGroupsStrongCounter(asyncCounter, configuration, this));
   }

   @Override
   public CompletionStage<Void> apply(String counterName) {
      // remove function !!
      return removeStrongCounter(counterName);
   }
}
