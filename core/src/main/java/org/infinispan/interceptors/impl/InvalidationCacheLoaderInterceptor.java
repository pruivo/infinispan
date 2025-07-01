package org.infinispan.interceptors.impl;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.lang.invoke.MethodHandles;

/**
 * A cache loader interceptor for cache type {@link org.infinispan.configuration.cache.CacheType#INVALIDATION}.
 *
 * @since 15.0
 */
public class InvalidationCacheLoaderInterceptor<K, V> extends CacheLoaderInterceptor<K, V> {

   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   @Override
   protected boolean skipLoadForWriteCommand(WriteCommand cmd, Object key, InvocationContext ctx) {
      if (cmd.hasAnyFlag(FlagBitSets.SKIP_CACHE_LOAD)  ||
              cmd.loadType() == VisitableCommand.LoadType.DONT_LOAD ||
              cmd instanceof InvalidateCommand) {
         if (log.isTraceEnabled()) {
            log.tracef("Skip load for command %s.", cmd);
         }
         return true;
      }

      return false;
   }

}
