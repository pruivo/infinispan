package org.infinispan.interceptors.impl;

import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.context.InvocationContext;

public class InvalidationCacheWriterInterceptor extends CacheWriterInterceptor {

    @Override
    boolean shouldReplicateRemove(InvocationContext ctx, RemoveCommand removeCommand) {
        // in invalidation mode, the key needs to be removed from persistence because the other nodes will only receive an invalidation command.
        return true;
    }
}
