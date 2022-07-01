package org.infinispan.counter.impl.factory;

import java.util.concurrent.CompletionStage;

import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.impl.CounterModuleLifecycle;
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

/**
 * TODO!
 */
@Scope(Scopes.GLOBAL)
public class RaftStrongCounterFactory implements StrongCounterFactory {

   @Inject Transport transport;
   private RaftCountersStateMachine stateMachine;

   @Start
   public void start() {
      stateMachine = transport.raftManager().getOrRegisterStateMachine(CounterModuleLifecycle.RAFT_COUNTER_CHANNEL_ID, RaftCountersStateMachine::new, new RaftChannelConfiguration.Builder().logMode(RaftChannelConfiguration.RaftLogMode.VOLATILE).build());
   }

   @Override
   public CompletionStage<Void> removeStrongCounter(String name) {
      return stateMachine.sendOperation(new ResetOperation(ByteString.fromString(name))).thenRun(() -> {
      });
   }

   @Override
   public CompletionStage<InternalCounterAdmin> createStrongCounter(String name, CounterConfiguration configuration) {
      return stateMachine.createIfAbsent(name, configuration);
   }
}
