package org.infinispan.util.concurrent.locks.order;

import org.infinispan.commands.remote.CacheRpcCommand;

import java.util.Collection;

/**
 * Simple interface to extract all the keys that may need to be locked.
 * <p/>
 * A {@link org.infinispan.commands.remote.CacheRpcCommand} that needs to acquire locks should implement this interface.
 * This way, the {@link org.infinispan.util.concurrent.locks.order.RemoteLockOrderManager} tries to manage the command
 * to optimize the system resources usage.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public interface RemoteLockCommand {

   /**
    * For commands that need to lock all the key set (e.g. the {@link org.infinispan.commands.write.ClearCommand}).
    */
   public static Collection<Object> ALL_KEYS = null;

   /**
    * @return a {@link java.util.Collection} of keys to lock. It must return {@link #ALL_KEYS} when need to lock all the
    * key set and it can return an empty collection when no keys need to be locked.
    */
   Collection<Object> getKeysToLock();


}
