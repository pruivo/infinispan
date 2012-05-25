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

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.ClusteredRepeatableReadEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.IncrementableEntryVersion;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 4.0
 */
public class TotalOrderVersionedCommitContextEntries extends NonVersionedCommitContextEntries {

   private final Log log = LogFactory.getLog(TotalOrderVersionedCommitContextEntries.class);
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
      if (ctx.isInTxScope()) {
         ClusteredRepeatableReadEntry clusterMvccEntry = (ClusteredRepeatableReadEntry) entry;
         EntryVersion existingVersion = clusterMvccEntry.getVersion();

         EntryVersion newVersion;
         if (existingVersion == null) {
            newVersion = versionGenerator.generateNew();
         } else {
            newVersion = versionGenerator.increment((IncrementableEntryVersion) existingVersion);
         }
         clusteringDependentLogic.commitEntry(entry, newVersion, skipOwnershipCheck, ctx);
      } else {
         // This could be a state transfer call!
         clusteringDependentLogic.commitEntry(entry, entry.getVersion(), skipOwnershipCheck, ctx);
      }
   }
}
