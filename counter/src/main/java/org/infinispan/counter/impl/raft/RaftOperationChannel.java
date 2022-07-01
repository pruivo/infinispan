package org.infinispan.counter.impl.raft;

import java.util.concurrent.CompletionStage;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.counter.impl.raft.operation.RaftCounterOperation;

/**
 * TODO!
 */
public interface RaftOperationChannel {

   CompletionStage<ByteBuffer> sendOperation(RaftCounterOperation<?> operation);

}
