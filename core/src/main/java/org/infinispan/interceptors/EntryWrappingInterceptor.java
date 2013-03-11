/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
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

package org.infinispan.interceptors;

import org.infinispan.commands.AbstractVisitor;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.*;
import org.infinispan.container.CommitContextEntries;
import org.infinispan.container.DataContainer;
import org.infinispan.container.EntryFactory;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Interceptor in charge with wrapping entries and add them in caller's context.
 *
 * @author Mircea Markus
 * @author Pedro Ruivo
 * @since 5.1
 */
public class EntryWrappingInterceptor extends CommandInterceptor {

   protected EntryFactory entryFactory;
   protected DataContainer dataContainer;
   protected ClusteringDependentLogic cll;
   protected CommitContextEntries commitContextEntries;
   protected final EntryWrappingVisitor entryWrappingVisitor = new EntryWrappingVisitor();

   private static final Log log = LogFactory.getLog(EntryWrappingInterceptor.class);

   @Override
   protected Log getLog() {
      return log;
   }

   @Inject
   public void init(EntryFactory entryFactory, DataContainer dataContainer, ClusteringDependentLogic cll,
                    CommitContextEntries commitContextEntries) {
      this.entryFactory =  entryFactory;
      this.dataContainer = dataContainer;
      this.cll = cll;
      this.commitContextEntries = commitContextEntries;
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      wrapEntriesForPrepare(ctx, command);
      Object result = invokeNextInterceptor(ctx, command);
      //new commit conditions for total order
      if (shouldCommitEntries(command, ctx)) {
         commitContextEntries.commitContextEntries(ctx);
      }
      return result;
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      try {
         return invokeNextInterceptor(ctx, command);
      } finally {
         commitContextEntries.commitContextEntries(ctx);
      }
   }

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      try {
         checkIfKeyRead(ctx, command.getKey(), command);
         entryFactory.wrapEntryForReading(ctx, command.getKey());
         return invokeNextInterceptor(ctx, command);
      } finally {
         //needed because entries might be added in L1
         if (!ctx.isInTxScope()) commitContextEntries.commitContextEntries(ctx);
      }
   }

   @Override
   public Object visitInvalidateCommand(InvocationContext ctx, InvalidateCommand command) throws Throwable {
      if (command.getKeys() != null) {
         for (Object key : command.getKeys())
            entryFactory.wrapEntryForReplace(ctx, key);
      }
      return invokeNextAndApplyChanges(ctx, command);
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      for (InternalCacheEntry entry : dataContainer.entrySet(null))
         entryFactory.wrapEntryForClear(ctx, entry.getKey());
      return invokeNextAndApplyChanges(ctx, command);
   }

   @Override
   public Object visitInvalidateL1Command(InvocationContext ctx, InvalidateL1Command command) throws Throwable {
      for (Object key : command.getKeys()) {
         entryFactory.wrapEntryForReplace(ctx, key);
      }
      return invokeNextAndApplyChanges(ctx, command);
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      entryFactory.wrapEntryForPut(ctx, command.getKey(), null, !command.isPutIfAbsent());
      checkIfKeyRead(ctx, command.getKey(), command);
      return invokeNextAndApplyChanges(ctx, command);
   }

   @Override
   public Object visitApplyDeltaCommand(InvocationContext ctx, ApplyDeltaCommand command) throws Throwable {
      entryFactory.wrapEntryForDelta(ctx, command.getDeltaAwareKey(), command.getDelta());
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      entryFactory.wrapEntryForRemove(ctx, command.getKey());
      checkIfKeyRead(ctx, command.getKey(), command);
      return invokeNextAndApplyChanges(ctx, command);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      entryFactory.wrapEntryForReplace(ctx, command.getKey());
      checkIfKeyRead(ctx, command.getKey(), command);
      return invokeNextAndApplyChanges(ctx, command);
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      for (Object key : command.getMap().keySet()) {
         entryFactory.wrapEntryForPut(ctx, key, null, true);
      }
      return invokeNextAndApplyChanges(ctx, command);
   }

   @Override
   public Object visitEvictCommand(InvocationContext ctx, EvictCommand command) throws Throwable {
      return visitRemoveCommand(ctx, command);
   }

   protected final void wrapEntriesForPrepare(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      if (!ctx.isOriginLocal() || command.isReplayEntryWrapping()) {
         for (WriteCommand c : command.getModifications()) c.acceptVisitor(ctx, entryWrappingVisitor);
      }
   }

   private Object invokeNextAndApplyChanges(InvocationContext ctx, VisitableCommand command) throws Throwable {
      final Object result = invokeNextInterceptor(ctx, command);
      if (!ctx.isInTxScope()) commitContextEntries.commitContextEntries(ctx);
      return result;
   }

   protected class EntryWrappingVisitor extends AbstractVisitor {

      @Override
      public final Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
         boolean notWrapped = false;
         for (Object key : dataContainer.keySet(null)) {
            wrapEntryForClear(ctx, key, command);
            notWrapped = true;
         }
         if (notWrapped)
            invokeNextInterceptor(ctx, command);
         return null;
      }

      @Override
      public final Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
         boolean notWrapped = false;
         for (Object key : command.getMap().keySet()) {
            if (cll.localNodeIsOwner(key)) {
               wrapEntryForPut(ctx, key, false, command);
               notWrapped = true;
            }
         }
         if (notWrapped)
            invokeNextInterceptor(ctx, command);
         return null;
      }

      @Override
      public final Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
         if (cll.localNodeIsOwner(command.getKey())) {
            wrapEntryForRemove(ctx, command.getKey(), command);
            invokeNextInterceptor(ctx, command);
         }
         return null;
      }

      @Override
      public final Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
         if (cll.localNodeIsOwner(command.getKey())) {
            wrapEntryForPut(ctx, command.getKey(), command.isPutIfAbsent(), command);
            invokeNextInterceptor(ctx, command);
         }
         return null;
      }

      @Override
      public final Object visitApplyDeltaCommand(InvocationContext ctx, ApplyDeltaCommand command) throws Throwable {
         if (cll.localNodeIsOwner(command.getKey())) {
            entryFactory.wrapEntryForDelta(ctx, command.getDeltaAwareKey(), command.getDelta());
            invokeNextInterceptor(ctx, command);
         }
         return null;
      }

      @Override
      public final Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
         if (cll.localNodeIsOwner(command.getKey())) {
            wrapEntryForReplace(ctx, command.getKey(), command);
            invokeNextInterceptor(ctx, command);
         }
         return null;
      }

      protected MVCCEntry wrapEntryForReplace(InvocationContext ctx, Object key, ReplaceCommand command) throws InterruptedException {
         return entryFactory.wrapEntryForReplace(ctx, key);
      }

      protected MVCCEntry wrapEntryForPut(InvocationContext ctx, Object key, boolean putIfAbsent, WriteCommand command) throws InterruptedException {
         return entryFactory.wrapEntryForPut(ctx, key, null, !putIfAbsent);
      }

      protected MVCCEntry wrapEntryForRemove(InvocationContext ctx, Object key, RemoveCommand command) throws InterruptedException {
         return entryFactory.wrapEntryForRemove(ctx, key);
      }

      protected MVCCEntry wrapEntryForClear(InvocationContext ctx, Object key, ClearCommand command) throws InterruptedException {
         return entryFactory.wrapEntryForClear(ctx, key);
      }
   }

   /**
    * total order condition: only commits when it is remote context and the prepare has the flag 1PC set
    * 2PC condition: only commits if the prepare has the flag 1PC set
    *
    * @param command the prepare command
    * @param ctx the invocation context
    * @return true if the modification should be committed, false otherwise
    */
   protected boolean shouldCommitEntries(PrepareCommand command, TxInvocationContext ctx) {
      //one phase commit in remote context in total order or it has no modifications (local commands)
      boolean totalOrder = command.getGlobalTransaction().getReconfigurableProtocol().useTotalOrder();
      return (totalOrder && command.isOnePhaseCommit() &&
                    (!ctx.isOriginLocal() || !ctx.hasModifications())) ||
            //original condition: one phase commit
            (!totalOrder && command.isOnePhaseCommit());
   }

   protected void checkIfKeyRead(InvocationContext context, Object key, VisitableCommand command) {
      //no-op, it is only needed to check the write skew
   }
}
