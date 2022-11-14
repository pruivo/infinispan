package org.infinispan.counter.impl.factory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterType;
import org.infinispan.counter.api.Storage;
import org.infinispan.counter.impl.jgroups.UnboundedJGroupsStrongCounter;
import org.infinispan.counter.impl.manager.InternalCounterAdmin;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.transport.jgroups.JGroupsRaftManager;
import org.jgroups.raft.blocks.CounterService;

/**
 * TODO!
 */
@Scope(Scopes.GLOBAL)
public class RaftJGroupsCounterFactory implements StrongCounterFactory, Function<String, CompletionStage<Void>> {

   private final CounterService counterService;

   private RaftJGroupsCounterFactory(CounterService counterService) {
      this.counterService = counterService;
   }

   public static RaftJGroupsCounterFactory create(JGroupsRaftManager raftManager) {
      CounterService counterService = raftManager.getOrCreateRaftCounterService();
      return new RaftJGroupsCounterFactory(counterService);
   }

   @Override
   public CompletionStage<Void> removeStrongCounter(String counterName) {
      //TODO non-blocking method missing!
      try {
         counterService.deleteCounter(counterName);
      } catch (Exception e) {
         return CompletableFuture.failedFuture(e);
      }
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
      if (configuration.storage() == Storage.VOLATILE) {
         throw new IllegalArgumentException();
      }
      return counterService.getOrCreateAsyncCounter(counterName, configuration.initialValue())
            .thenApply(counter -> new UnboundedJGroupsStrongCounter(counter, configuration, this));
   }

   @Override
   public CompletionStage<Void> apply(String counterName) {
      //remove function
      return removeStrongCounter(counterName);
   }
}
