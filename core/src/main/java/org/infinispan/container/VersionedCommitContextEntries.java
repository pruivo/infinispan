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

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.ClusteredRepeatableReadEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.IncrementableEntryVersion;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class VersionedCommitContextEntries extends NonVersionedCommitContextEntries {

   private final Log log = LogFactory.getLog(VersionedCommitContextEntries.class);
   private VersionGenerator versionGenerator;

   @Inject
   public final void injectVersionGenerator(VersionGenerator versionGenerator) {
      this.versionGenerator = versionGenerator;
   }

   @Override
   protected Log getLog() {
      return log;
   }

   @Override
   protected void commitContextEntry(CacheEntry entry, InvocationContext ctx, boolean skipOwnershipCheck) {
      if (ctx.isInTxScope() && !isFromStateTransfer(ctx)) {
         ClusteredRepeatableReadEntry clusterMvccEntry = (ClusteredRepeatableReadEntry) entry;
         EntryVersion version;

         if (((TxInvocationContext) ctx).getGlobalTransaction().getReconfigurableProtocol().useTotalOrder()) {
            EntryVersion existingVersion = clusterMvccEntry.getVersion();
            if (existingVersion == null) {
               version = versionGenerator.generateNew();
            } else {
               version = versionGenerator.increment((IncrementableEntryVersion) existingVersion);
            }
         } else {
            version = ((TxInvocationContext) ctx).getCacheTransaction().getUpdatedEntryVersions().get(entry.getKey());
         }

         clusteringDependentLogic.commitEntry(entry, version, skipOwnershipCheck, ctx);
      } else {
         // This could be a state transfer call!
         clusteringDependentLogic.commitEntry(entry, entry.getVersion(), skipOwnershipCheck, ctx);
      }
   }

   protected boolean isFromStateTransfer(InvocationContext ctx) {
      if (ctx.isInTxScope() && ctx.isOriginLocal()) {
         LocalTransaction localTx = (LocalTransaction) ((TxInvocationContext) ctx).getCacheTransaction();
         if (localTx.isFromStateTransfer()) {
            return true;
         }
      }
      return false;
   }
}
