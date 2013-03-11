/*
 * Copyright 2012 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

package org.infinispan.transaction;

import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.ClusteredRepeatableReadEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.container.versioning.IncrementableEntryVersion;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.transaction.xa.CacheTransaction;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Encapsulates write skew logic in maintaining version maps, etc.
 *
 * @author Manik Surtani
 * @since 5.1
 */
public class WriteSkewHelper {

   public static void setVersionsSeenOnPrepareCommand(VersionedPrepareCommand command, TxInvocationContext context) {
      // Build a map of keys to versions as they were seen by the transaction originator's transaction context
      EntryVersionsMap vs = new EntryVersionsMap();
      for (WriteCommand wc : command.getModifications()) {
         for (Object k : wc.getAffectedKeys()) {
            CacheEntry cacheEntry = context.lookupEntry(k);
            if (cacheEntry != null) //null is possible in the case the entry didn't exist
               vs.put(k, (IncrementableEntryVersion) cacheEntry.getVersion());
         }
      }

      // Make sure this version map is attached to the prepare command so that lock owners can perform write skew checks
      command.setVersionsSeen(vs);
   }

   public static void setNewVersionsOnPrepareCommand(VersionedPrepareCommand command, TxInvocationContext context) {
      // Build a map of keys to versions as they were seen by the transaction originator's transaction context
      EntryVersionsMap vs = new EntryVersionsMap();
      EntryVersionsMap newVersions = context.getCacheTransaction().getUpdatedEntryVersions();
      for (WriteCommand wc : command.getModifications()) {
         for (Object k : wc.getAffectedKeys()) {
            vs.put(k, newVersions.get(k));
         }
      }

      // Make sure this version map is attached to the prepare command so that owners can apply the correct version
      command.setVersionsSeen(vs);
   }

   public static void readVersionsFromResponse(Response r, CacheTransaction ct) {
      if (r != null && r.isSuccessful()) {
         SuccessfulResponse sr = (SuccessfulResponse) r;
         EntryVersionsMap uv = (EntryVersionsMap) sr.getResponseValue();
         if (uv != null) ct.setUpdatedEntryVersions(uv.merge(ct.getUpdatedEntryVersions()));
      }
   }

   public static EntryVersionsMap performWriteSkewCheckAndReturnNewVersions(VersionedPrepareCommand prepareCommand,
                                                                            DataContainer dataContainer,
                                                                            VersionGenerator versionGenerator,
                                                                            TxInvocationContext context,
                                                                            ClusteringDependentLogic cdl) {
      EntryVersionsMap uv = new EntryVersionsMap();
      for (WriteCommand c : prepareCommand.getModifications()) {
         for (Object k : c.getAffectedKeys()) {
            if (cdl.localNodeIsOwner(k)) {
               ClusteredRepeatableReadEntry entry = (ClusteredRepeatableReadEntry) context.lookupEntry(k);

               if (!context.isOriginLocal()) {
                  // What version did the transaction originator see??
                  EntryVersion versionSeen = prepareCommand.getVersionsSeen().get(k);
                  if (versionSeen != null) entry.setVersion(versionSeen);
               }

               if (entry.performWriteSkewCheck(dataContainer, c.wasPreviousRead())) {
                  IncrementableEntryVersion newVersion = entry.isCreated() ? versionGenerator.generateNew() : versionGenerator.increment((IncrementableEntryVersion) entry.getVersion());
                  uv.put(k, newVersion);
               } else {
                  InternalCacheEntry actual = dataContainer.get(entry.getKey(), null);
                  // Write skew check detected!
                  throw WriteSkewException.createException(k, actual, entry, prepareCommand.getGlobalTransaction());
               }
            }
         }
      }
      return uv;
   }

   public static interface KeySpecificLogic {
      boolean performCheckOnKey(Object key);
   }

   public static void updateLocalModeCacheEntries(VersionGenerator versionGenerator, VersionedPrepareCommand command,
                                                  TxInvocationContext context) {
      Set<Object> keysChecked = new HashSet<Object>();
      for (WriteCommand writeCommand : command.getModifications()) {
         keysChecked.addAll(writeCommand.getAffectedKeys());
      }

      EntryVersionsMap uv = context.getCacheTransaction().getUpdatedEntryVersions();      

      for (Map.Entry<Object, CacheEntry> entry : context.getLookedUpEntries().entrySet()) {
         if (keysChecked.contains(entry.getKey()) || !entry.getValue().isChanged()) {
            continue;
         }
         IncrementableEntryVersion version = (IncrementableEntryVersion) entry.getValue().getVersion();
         if (version == null) {
            version = versionGenerator.generateNew();
         } else {
            version = versionGenerator.increment(version);
         }
         if (uv == null) {
            uv = new EntryVersionsMap();
         }
         uv.put(entry.getKey(), version);
      }
      context.getCacheTransaction().setUpdatedEntryVersions(uv);
   }
}
