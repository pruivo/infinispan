/*
 * INESC-ID, Instituto de Engenharia de Sistemas e Computadores Investigação e Desevolvimento em Lisboa
 * Copyright 2013 INESC-ID and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 3.0 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.container;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.SingleKeyNonTxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class NonVersionedCommitContextEntries implements CommitContextEntries {

   private final Log log = LogFactory.getLog(NonVersionedCommitContextEntries.class);
   private Configuration configuration;
   private DistributionManager distributionManager;
   private DataContainer dataContainer;

   @Inject
   public void inject(Configuration configuration, DistributionManager distributionManager, DataContainer dataContainer) {
      this.configuration = configuration;
      this.distributionManager = distributionManager;
      this.dataContainer = dataContainer;
   }

   @Override
   public final void commitContextEntries(InvocationContext context) {
      final Log log = getLog();
      final boolean trace = log.isTraceEnabled();
      boolean skipOwnershipCheck = context.hasFlag(Flag.SKIP_OWNERSHIP_CHECK);

      if (context instanceof SingleKeyNonTxInvocationContext) {
         CacheEntry entry = ((SingleKeyNonTxInvocationContext) context).getCacheEntry();
         commitEntryIfNeeded(context, skipOwnershipCheck, entry);
      } else {
         Set<Map.Entry<Object, CacheEntry>> entries = context.getLookedUpEntries().entrySet();
         Iterator<Map.Entry<Object, CacheEntry>> it = entries.iterator();
         while (it.hasNext()) {
            Map.Entry<Object, CacheEntry> e = it.next();
            CacheEntry entry = e.getValue();
            if (!commitEntryIfNeeded(context, skipOwnershipCheck, entry)) {
               if (trace) {
                  if (entry == null)
                     log.tracef("Entry for key %s is null : not calling commitUpdate", e.getKey());
                  else
                     log.tracef("Entry for key %s is not changed(%s): not calling commitUpdate", e.getKey(), entry);
               }
            }
         }
      }
   }

   protected Log getLog() {
      return log;
   }

   @Override
   public void commitContextEntry(CacheEntry entry, InvocationContext ctx, boolean skipOwnershipCheck) {
      commitEntry(entry, null, skipOwnershipCheck);
   }

   protected final void commitEntry(CacheEntry entry, EntryVersion newVersion, boolean skipOwnershipCheck) {
      if (configuration.clustering().cacheMode().isDistributed()) {
         commitDistributedEntry(entry, newVersion, skipOwnershipCheck);
      } else {
         commitReplicatedEntry(entry, newVersion);
      }
   }

   private boolean commitEntryIfNeeded(InvocationContext ctx, boolean skipOwnershipCheck, CacheEntry entry) {
      Log log = getLog();
      boolean isLoaded = entry instanceof MVCCEntry && ((MVCCEntry) entry).isLoaded();
      if (entry != null && (entry.isChanged() || isLoaded)) {
         if (log.isTraceEnabled()) {
            log.tracef("Entry has changed. Committing %s", entry);
         }
         commitContextEntry(entry, ctx, skipOwnershipCheck);
         if (log.isTraceEnabled()) {
            log.tracef("Committed entry %s", entry);
         }
         return true;
      }
      return false;
   }

   private void commitDistributedEntry(CacheEntry entry, EntryVersion newVersion, boolean skipOwnershipCheck) {
      boolean doCommit = true;
      boolean local = distributionManager.getLocality(entry.getKey()).isLocal();
      // ignore locality for removals, even if skipOwnershipCheck is not true
      if (!skipOwnershipCheck && !entry.isRemoved() && !distributionManager.getLocality(entry.getKey()).isLocal()) {
         if (configuration.clustering().l1().enabled()) {
            distributionManager.transformForL1(entry);
         } else {
            doCommit = false;
         }
      }
      Log log = getLog();
      if (log.isTraceEnabled()) {
         log.tracef("Trying to commit entry in distributed mode. commit?=%s, local?=%s, key=%s", doCommit, local,
                    entry.getKey());
      }
      if (doCommit) {
         entry.commit(dataContainer, newVersion);
      } else {
         entry.rollback();
      }
   }

   private void commitReplicatedEntry(CacheEntry entry, EntryVersion newVersion) {
      Log log = getLog();
      if (log.isTraceEnabled()) {
         log.tracef("Trying to commit entry in replicated mode. key=%s", entry.getKey());
      }
      entry.commit(dataContainer, newVersion);
   }
}
