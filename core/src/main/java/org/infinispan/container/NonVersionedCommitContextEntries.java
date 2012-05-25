/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.container;

import org.infinispan.commands.write.ClearCommand;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.SingleKeyNonTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.statetransfer.StateConsumer;
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
   private StateConsumer stateConsumer;
   protected ClusteringDependentLogic clusteringDependentLogic;

   @Inject
   public void inject(StateConsumer stateConsumer, ClusteringDependentLogic clusteringDependentLogic) {
      this.stateConsumer = stateConsumer;
      this.clusteringDependentLogic = clusteringDependentLogic;
   }

   @Override
   public final void commitContextEntries(InvocationContext context, boolean skipOwnershipCheck, boolean isPutForStateTransfer) {
      final Log log = getLog();
      final boolean trace = log.isTraceEnabled();

      if (!isPutForStateTransfer && stateConsumer != null
            && context instanceof TxInvocationContext
            && ((TxInvocationContext) context).getCacheTransaction().hasModification(ClearCommand.class)) {
         // If we are committing a ClearCommand now then no keys should be written by state transfer from
         // now on until current rebalance ends.
         stateConsumer.stopApplyingState();
      }

      if (context instanceof SingleKeyNonTxInvocationContext) {
         SingleKeyNonTxInvocationContext singleKeycontext = (SingleKeyNonTxInvocationContext) context;
         commitEntryIfNeeded(context, skipOwnershipCheck, singleKeycontext.getKey(), singleKeycontext.getCacheEntry(), isPutForStateTransfer);
      } else {
         Set<Map.Entry<Object, CacheEntry>> entries = context.getLookedUpEntries().entrySet();
         Iterator<Map.Entry<Object, CacheEntry>> it = entries.iterator();
         while (it.hasNext()) {
            Map.Entry<Object, CacheEntry> e = it.next();
            CacheEntry entry = e.getValue();
            if (!commitEntryIfNeeded(context, skipOwnershipCheck, e.getKey(), entry, isPutForStateTransfer)) {
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

   protected void commitContextEntry(CacheEntry entry, InvocationContext ctx, boolean skipOwnershipCheck) {
      clusteringDependentLogic.commitEntry(entry, null, skipOwnershipCheck, ctx);
   }

   private boolean commitEntryIfNeeded(InvocationContext ctx, boolean skipOwnershipCheck, Object key, CacheEntry entry, boolean isPutForStateTransfer) {
      if (entry == null) {
         if (key != null && !isPutForStateTransfer && stateConsumer != null) {
            // this key is not yet stored locally
            stateConsumer.addUpdatedKey(key);
         }
         return false;
      }

      if (isPutForStateTransfer && stateConsumer.isKeyUpdated(key)) {
         // This is a state transfer put command on a key that was already modified by other user commands. We need to back off.
         entry.rollback();
         return false;
      }

      if (entry.isChanged() || entry.isLoaded()) {
         log.tracef("About to commit entry %s", entry);
         commitContextEntry(entry, ctx, skipOwnershipCheck);

         if (!isPutForStateTransfer && stateConsumer != null) {
            stateConsumer.addUpdatedKey(key);
         }

         return true;
      }
      return false;
   }
}
