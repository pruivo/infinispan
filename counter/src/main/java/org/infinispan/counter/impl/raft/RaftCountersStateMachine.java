package org.infinispan.counter.impl.raft;

import static org.infinispan.commons.util.concurrent.CompletableFutures.completedExceptionFuture;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.io.EmptyByteBuffer;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterType;
import org.infinispan.counter.impl.manager.InternalCounterAdmin;
import org.infinispan.counter.impl.raft.operation.CreateCounterOperation;
import org.infinispan.counter.impl.raft.operation.OperationCode;
import org.infinispan.counter.impl.raft.operation.RaftCounterOperation;
import org.infinispan.counter.logging.Log;
import org.infinispan.remoting.transport.raft.RaftChannel;
import org.infinispan.remoting.transport.raft.RaftStateMachine;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.NonBlockingManager;
import org.infinispan.util.logging.LogFactory;

/**
 * TODO!
 */
public class RaftCountersStateMachine implements RaftStateMachine, RaftOperationChannel {

   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass(), Log.class);

   private RaftChannel raftChannel;
   private final Map<ByteString, CompletableFuture<RaftCounter>> counters = new ConcurrentHashMap<>(16);
   private final NonBlockingManager nonBlockingManager;

   public RaftCountersStateMachine(NonBlockingManager nonBlockingManager) {
      this.nonBlockingManager = nonBlockingManager;
   }

   @Override
   public void init(RaftChannel raftChannel) {
      this.raftChannel = raftChannel;
   }

   @Override
   public ByteBuffer apply(ByteBuffer buffer) throws Exception {
      RaftCounterOperation<?> operation = OperationCode.decode(buffer);
      if (log.isTraceEnabled()) {
         log.tracef("Received raft-counter operation: %s", operation);
      }
      ByteString name = operation.getCounterName();
      if (operation instanceof CreateCounterOperation) {
         RaftCounter counter = new RaftCounter(name, this, ((CreateCounterOperation) operation).getConfiguration());
         CompletableFuture<RaftCounter> existing = counters.putIfAbsent(name, CompletableFuture.completedFuture(counter));
         if (existing != null) {
            nonBlockingManager.complete(existing, counter);
         }
         return EmptyByteBuffer.INSTANCE;
      }
      CompletableFuture<RaftCounter> cf = counters.get(name);
      assert cf != null;
      assert cf.isDone();
      assert !cf.isCompletedExceptionally();
      return operation.execute(cf.join());
   }

   @Override
   public void readStateFrom(DataInput dataInput) {

   }

   @Override
   public void writeStateTo(DataOutput dataOutput) {

   }

   @Override
   public CompletionStage<ByteBuffer> sendOperation(RaftCounterOperation<?> operation) {
      if (log.isTraceEnabled()) {
         log.tracef("Sending raft-counter operation: %s", operation);
      }
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
      CompletableFuture<RaftCounter> counterFuture = new CompletableFuture<>();
      CompletableFuture<RaftCounter> existing = counters.putIfAbsent(counterName, counterFuture);
      if (existing != null) {
         return existing.thenApply(CompletableFutures.identity());
      }
      // response not required
      sendOperation(op);
      return counterFuture.thenApply(CompletableFutures.identity());
   }

}
