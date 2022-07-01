package org.infinispan.counter.impl.raft;

import static org.infinispan.commons.util.concurrent.CompletableFutures.completedExceptionFuture;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.io.EmptyByteBuffer;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterType;
import org.infinispan.counter.impl.manager.InternalCounterAdmin;
import org.infinispan.counter.impl.raft.operation.CreateCounterOperation;
import org.infinispan.counter.impl.raft.operation.OperationCode;
import org.infinispan.counter.impl.raft.operation.RaftCounterOperation;
import org.infinispan.remoting.transport.raft.RaftChannel;
import org.infinispan.remoting.transport.raft.RaftStateMachine;
import org.infinispan.util.ByteString;

/**
 * TODO!
 */
public class RaftCountersStateMachine implements RaftStateMachine, RaftOperationChannel {

   private RaftChannel raftChannel;
   private final Map<ByteString, RaftCounter> counters = new ConcurrentHashMap<>(16);

   @Override
   public void init(RaftChannel raftChannel) {
      this.raftChannel = raftChannel;
   }

   @Override
   public ByteBuffer apply(ByteBuffer buffer) throws Exception {
      RaftCounterOperation<?> operation = OperationCode.decode(buffer);
      ByteString name = operation.getCounterName();
      if (operation instanceof CreateCounterOperation) {
         counters.computeIfAbsent(name, s -> new RaftCounter(s, this, ((CreateCounterOperation) operation).getConfiguration()));
         return EmptyByteBuffer.INSTANCE;
      }
      RaftCounter counter = counters.get(name);
      return operation.execute(counter);
   }

   @Override
   public void readStateFrom(DataInput dataInput) {

   }

   @Override
   public void writeStateTo(DataOutput dataOutput) {

   }

   @Override
   public CompletionStage<ByteBuffer> sendOperation(RaftCounterOperation<?> operation) {
      try {
         return raftChannel.send(OperationCode.encode(operation));
      } catch (IOException e) {
         return completedExceptionFuture(e);
      }
   }

   public CompletionStage<InternalCounterAdmin> createIfAbsent(String name, CounterConfiguration configuration) {
      assert configuration.type() == CounterType.BOUNDED_STRONG || configuration.type() == CounterType.UNBOUNDED_STRONG;
      ByteString counterName = ByteString.fromString(name);
      CreateCounterOperation op = new CreateCounterOperation(counterName, configuration);
      return sendOperation(op).thenApply(buffer -> counters.get(counterName));
   }
}
