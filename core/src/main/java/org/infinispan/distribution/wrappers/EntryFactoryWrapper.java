package org.infinispan.distribution.wrappers;

import org.infinispan.atomic.Delta;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.container.EntryFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @author Mircea Markus <mircea.markus@jboss.com> (C) 2011 Red Hat Inc.
 * @since 5.2
 */
public class EntryFactoryWrapper implements EntryFactory {
   private static final Log log = LogFactory.getLog(EntryFactoryWrapper.class);

   private final EntryFactory actual;

   public EntryFactoryWrapper(EntryFactory actual) {
      this.actual = actual;
   }

   @Override
   public CacheEntry wrapEntryForReading(InvocationContext ctx, Object key) throws InterruptedException {
      log.tracef("EntryFactoryWrapper.wrapEntryForReading");
      return actual.wrapEntryForReading(ctx, key);
   }

   @Override
   public MVCCEntry wrapEntryForClear(InvocationContext ctx, Object key) throws InterruptedException {
      log.tracef("EntryFactoryWrapper.wrapEntryForClear");
      return actual.wrapEntryForClear(ctx, key);
   }

   @Override
   public MVCCEntry wrapEntryForReplace(InvocationContext ctx, Object key) throws InterruptedException {
      log.tracef("EntryFactoryWrapper.wrapEntryForReplace");
      return actual.wrapEntryForReplace(ctx, key);
   }

   @Override
   public MVCCEntry wrapEntryForRemove(InvocationContext ctx, Object key) throws InterruptedException {
      log.tracef("EntryFactoryWrapper.wrapEntryForRemove");
      return actual.wrapEntryForRemove(ctx, key);
   }

   @Override
   public CacheEntry wrapEntryForDelta(InvocationContext ctx, Object deltaKey, Delta delta) throws InterruptedException {
      log.tracef("EntryFactoryWrapper.wrapEntryForDelta");
      return actual.wrapEntryForDelta(ctx, deltaKey, delta);
   }

   @Override
   public MVCCEntry wrapEntryForPut(InvocationContext ctx, Object key, InternalCacheEntry ice, boolean undeleteIfNeeded, FlagAffectedCommand cmd) throws InterruptedException {
      log.tracef("EntryFactoryWrapper.wrapEntryForPut");
      return actual.wrapEntryForPut(ctx, key, ice, undeleteIfNeeded, cmd);
   }   
}
