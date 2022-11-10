package org.infinispan.counter.impl.factory;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

import org.infinispan.AdvancedCache;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterType;
import org.infinispan.counter.api.StrongCounter;
import org.infinispan.counter.api.WeakCounter;
import org.infinispan.counter.impl.entries.CounterKey;
import org.infinispan.counter.impl.entries.CounterValue;
import org.infinispan.counter.impl.jgroups.UnboundedJGroupsStrongCounter;
import org.infinispan.counter.impl.manager.InternalCounterAdmin;
import org.infinispan.counter.impl.strong.BoundedStrongCounter;
import org.infinispan.counter.impl.strong.StrongCounterKey;
import org.infinispan.counter.impl.weak.WeakCounterImpl;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.jgroups.JChannel;
import org.jgroups.blocks.atomic.Counter;
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
public class JGroupsCounterFactory implements StrongCounterFactory {

   private final ForkChannel forkChannel;
   private final CounterService counterService;

   public JGroupsCounterFactory(JGroupsTransport transport) throws Exception {
      this.forkChannel = createAndConnectForkChannel(transport);
      this.counterService = createCounterService(forkChannel);
   }

   private static ForkChannel createAndConnectForkChannel(JGroupsTransport transport) throws Exception {
      //TODO this is kind of hacking/bad... we are breaking the Transport abstraction :(
      String stackId = "org.infinispan.COUNTER";
      JChannel channel = transport.getChannel();
      Protocol top = channel.getProtocolStack().getTopProtocol();
      //TODO need a way to configure the number of backups!
      ForkChannel forkChannel = new ForkChannel(channel, stackId, stackId, true, ProtocolStack.Position.ABOVE,
            top.getClass(), new COUNTER());
      forkChannel.connect(stackId);
      return forkChannel;
   }

   private static CounterService createCounterService(ForkChannel forkChannel) {
      if (forkChannel == null) {
         return null;
      }
      //TODO load jgroups counters somehow.
      return new CounterService(forkChannel);
   }

   @Stop
   public void stop() {
      forkChannel.disconnect();
   }

   @Override
   public CompletionStage<Void> removeStrongCounter(String counterName) {
      counterService.deleteCounter(counterName);
      return null;
   }

   @Override
   public CompletionStage<InternalCounterAdmin> createStrongCounter(String counterName, CounterConfiguration configuration) {
      if (configuration.type() == CounterType.BOUNDED_STRONG) {
         throw new UnsupportedOperationException();
      }
      assert configuration.type() != CounterType.WEAK;
      return counterService.getOrCreateAsyncCounter(counterName, configuration.initialValue()).thenApply(asyncCounter -> new UnboundedJGroupsStrongCounter(asyncCounter, configuration));
   }
}
