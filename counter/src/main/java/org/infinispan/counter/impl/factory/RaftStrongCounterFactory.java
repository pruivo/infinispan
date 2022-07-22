package org.infinispan.counter.impl.factory;

import static org.infinispan.counter.impl.CounterModuleLifecycle.RAFT_COUNTER_CHANNEL_ID;

import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.impl.manager.InternalCounterAdmin;
import org.infinispan.counter.impl.raft.RaftCountersStateMachine;
import org.infinispan.counter.impl.raft.operation.ResetOperation;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.raft.RaftChannelConfiguration;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.NonBlockingManager;

/**
 * TODO!
 */
@Scope(Scopes.GLOBAL)
public class RaftStrongCounterFactory implements StrongCounterFactory, Supplier<RaftCountersStateMachine> {

   @Inject Transport transport;
   @Inject NonBlockingManager nonBlockingManager;
   private RaftCountersStateMachine stateMachine;

   @Start
   public void start() {
      stateMachine = transport.raftManager().getOrRegisterStateMachine(RAFT_COUNTER_CHANNEL_ID, this, raftConfiguration());
   }

   @Override
   public CompletionStage<Void> removeStrongCounter(String name) {
      return stateMachine.sendOperation(new ResetOperation(ByteString.fromString(name)))
            .thenApply(CompletableFutures.toVoidFunction());
   }

   @Override
   public CompletionStage<InternalCounterAdmin> createStrongCounter(String name, CounterConfiguration configuration) {
      return stateMachine.createIfAbsent(name, configuration);
   }

   @Override
   public RaftCountersStateMachine get() {
      return new RaftCountersStateMachine(nonBlockingManager);
   }

   private static RaftChannelConfiguration raftConfiguration() {
      return new RaftChannelConfiguration.Builder().logMode(RaftChannelConfiguration.RaftLogMode.VOLATILE).build();
   }
}
