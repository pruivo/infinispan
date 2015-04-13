package org.infinispan.util.concurrent.locks.order;

/**
 * Manages the execution order for the {@link org.infinispan.commands.remote.CacheRpcCommand} received.
 * <p/>
 * It tries to execute the commands in the order that originates less contention between them.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public interface RemoteLockOrderManager {

   /**
    * Orders the command and return a {@link org.infinispan.util.concurrent.locks.order.LockLatch} implementation. The
    * {@link org.infinispan.util.concurrent.locks.order.LockLatch} will change it state when the command is ready to be
    * processed.
    *
    * @param command the remote command
    * @return a {@link org.infinispan.util.concurrent.locks.order.LockLatch} implementation.
    */
   LockLatch order(RemoteLockCommand command);

}
