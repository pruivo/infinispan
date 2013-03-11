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
package org.infinispan.interceptors.totalorder;

import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.entries.ClusteredRepeatableReadEntry;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.VersionedEntryWrappingInterceptor;
import org.infinispan.transaction.WriteSkewException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * Wrapping Interceptor for Total Order protocol with versioning
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.2
 */
public class TotalOrderVersionedEntryWrappingInterceptor extends VersionedEntryWrappingInterceptor {

   private static final Log log = LogFactory.getLog(TotalOrderVersionedEntryWrappingInterceptor.class);
   private boolean trace;

   @Start
   public void setLogLevel() {
      trace = log.isTraceEnabled();
   }

   @Override
   public final Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {

      if (ctx.isOriginLocal()) {
         Object retVal = invokeNextInterceptor(ctx, command);
         if (shouldCommitEntries(command, ctx)) {
            commitContextEntries.commitContextEntries(ctx);
         }
         return retVal;
      }

      VersionCheckWrappingEntryVisitor visitor = new VersionCheckWrappingEntryVisitor(command);

      //both wraps and performs the write skew check
      if (!ctx.isOriginLocal() || command.isReplayEntryWrapping()) {
         for (WriteCommand c : command.getModifications())
            c.acceptVisitor(ctx, visitor);
      }

      invokeNextInterceptor(ctx, command);
      if (command.isOnePhaseCommit()) {
         commitContextEntries.commitContextEntries(ctx);
      } else {
         if (trace)
            log.tracef("Transaction %s will be committed in the 2nd phase", ctx.getGlobalTransaction().prettyPrint());
      }

      return configuration.getCacheMode().isReplicated() ? null : visitor.getKeysValidated();
   }

   public class VersionCheckWrappingEntryVisitor extends EntryWrappingVisitor {

      private final VersionedPrepareCommand prepareCommand;
      private final Set<Object> keysValidated;

      public VersionCheckWrappingEntryVisitor(PrepareCommand command) {
         this.prepareCommand = (VersionedPrepareCommand) command;
         this.keysValidated = new HashSet<Object>();
      }

      public Set<Object> getKeysValidated() {
         return keysValidated;
      }

      @Override
      protected final MVCCEntry wrapEntryForReplace(InvocationContext ctx, Object key, ReplaceCommand command) throws InterruptedException {
         return checkForWriteSkew(super.wrapEntryForReplace(ctx, key, command), command);
      }

      @Override
      protected final MVCCEntry wrapEntryForRemove(InvocationContext ctx, Object key, RemoveCommand command) throws InterruptedException {
         MVCCEntry mvccEntry = super.wrapEntryForRemove(ctx, key, command);
         if (mvccEntry != null) {
            return checkForWriteSkew(mvccEntry, command);
         } else {
            return null;
         }
      }

      @Override
      protected final MVCCEntry wrapEntryForPut(InvocationContext ctx, Object key, boolean putIfAbsent, WriteCommand command) throws InterruptedException {
         return checkForWriteSkew(super.wrapEntryForPut(ctx, key, putIfAbsent, command), command);
      }

      private MVCCEntry checkForWriteSkew(MVCCEntry mvccEntry, WriteCommand command) {
         ClusteredRepeatableReadEntry clusterMvccEntry = (ClusteredRepeatableReadEntry) mvccEntry;

         EntryVersionsMap versionsSeen = prepareCommand.getVersionsSeen();
         EntryVersion versionSeen = versionsSeen.get(clusterMvccEntry.getKey());

         if (versionSeen != null) {
            clusterMvccEntry.setVersion(versionSeen);
         }

         if (!clusterMvccEntry.performWriteSkewCheck(dataContainer, command.wasPreviousRead())) {
            throw WriteSkewException.createException(mvccEntry.getKey(), dataContainer.get(mvccEntry.getKey(), null),
                                                     mvccEntry, prepareCommand.getGlobalTransaction());
         }
         keysValidated.add(clusterMvccEntry.getKey());
         return clusterMvccEntry;
      }
   }
}
