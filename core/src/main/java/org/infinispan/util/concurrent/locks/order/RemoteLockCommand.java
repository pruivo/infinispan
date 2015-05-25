package org.infinispan.util.concurrent.locks.order;

import java.util.Collection;

/**
 * Simple interface to extract all the keys that may need to be locked.
 * <p>
 * A {@link org.infinispan.commands.remote.CacheRpcCommand} that needs to acquire locks should implement this interface.
 * This way, the {@link org.infinispan.util.concurrent.locks.order.RemoteLockOrderManager} tries to manage the command
 * to optimize the system resources usage.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public interface RemoteLockCommand {

   /**
    * @return a {@link java.util.Collection} of keys to lock. It returns an empty collection when no keys need to be locked.
    */
   Collection<Object> getKeysToLock();

   Object getLockOwner();

   boolean hasZeroLockAcquisition();

   boolean hasSkipLocking();
}
