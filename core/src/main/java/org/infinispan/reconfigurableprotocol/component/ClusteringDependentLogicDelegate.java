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
package org.infinispan.reconfigurableprotocol.component;

import org.infinispan.commands.tx.GMUPrepareCommand;
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.xa.CacheTransaction;

import java.util.Collection;
import java.util.Set;

/**
 * Delegates the method invocations for the correct instance depending of the protocol, for the ClusteringDependentLogic
 * component
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class ClusteringDependentLogicDelegate extends AbstractProtocolDependentComponent<ClusteringDependentLogic>
      implements ClusteringDependentLogic {


   @Override
   public boolean localNodeIsOwner(Object key) {
      return get().localNodeIsOwner(key);
   }

   @Override
   public boolean localNodeIsPrimaryOwner(Object key) {
      return get().localNodeIsPrimaryOwner(key);
   }

   @Override
   public Address getPrimaryOwner(Object key) {
      return get().getPrimaryOwner(key);
   }

   @Override
   public void commitEntry(CacheEntry entry, EntryVersion newVersion, boolean skipOwnershipCheck, InvocationContext ctx) {
      get().commitEntry(entry, newVersion, skipOwnershipCheck, ctx);
   }

   @Override
   public EntryVersionsMap createNewVersionsAndCheckForWriteSkews(VersionGenerator versionGenerator, TxInvocationContext context, VersionedPrepareCommand prepareCommand) {
      return get().createNewVersionsAndCheckForWriteSkews(versionGenerator, context, prepareCommand);
   }

   @Override
   public Address getAddress() {
      return get().getAddress();
   }

   @Override
   public Collection<Address> getOwners(Set<Object> affectedKeys) {
      return get().getOwners(affectedKeys);
   }

   @Override
   public Collection<Address> getInvolvedNodes(CacheTransaction cacheTransaction) {
      return get().getInvolvedNodes(cacheTransaction);
   }

   @Override
   public void performReadSetValidation(TxInvocationContext context, GMUPrepareCommand prepareCommand) {
      get().performReadSetValidation(context, prepareCommand);
   }

   @Override
   public Collection<Address> getWriteOwners(CacheTransaction cacheTransaction) {
      return get().getWriteOwners(cacheTransaction);
   }
}
