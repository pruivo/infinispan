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

import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.VersionedEntryWrappingInterceptor;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Wrapping Interceptor for Total Order protocol with versioning
 *
 * @author Mircea.Markus@jboss.com
 * @author Pedro Ruivo
 * @since 5.2
 */
public class TotalOrderVersionedEntryWrappingInterceptor extends VersionedEntryWrappingInterceptor {

   private static final Log log = LogFactory.getLog(TotalOrderVersionedEntryWrappingInterceptor.class);

   private static final EntryVersionsMap EMPTY_VERSION_MAP = new EntryVersionsMap();

   private boolean trace;

   @Start
   public void setLogLevel() {
      trace = log.isTraceEnabled();
   }

   @Override
   public final Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {

      if (ctx.isOriginLocal()) {
         //for local mode keys
         ctx.getCacheTransaction().setUpdatedEntryVersions(EMPTY_VERSION_MAP);
         return invokeNextInterceptor(ctx, command);
      }

      //Remote context, delivered in total order

      wrapEntriesForPrepare(ctx, command);

      Object retVal = invokeNextInterceptor(ctx, command);

      EntryVersionsMap versionsMap = cdl.createNewVersionsAndCheckForWriteSkews(versionGenerator, ctx,
                                                                                (VersionedPrepareCommand) command);

      if (command.isOnePhaseCommit()) {
         commitContextEntries.commitContextEntries(ctx, false, isFromStateTransfer(ctx));
      } else {
         if (trace)
            log.tracef("Transaction %s will be committed in the 2nd phase", ctx.getGlobalTransaction().prettyPrint());
      }

      return versionsMap == null ? retVal : versionsMap;
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      try {
         return invokeNextInterceptor(ctx, command);
      } finally {
         commitContextEntries.commitContextEntries(ctx, false, isFromStateTransfer(ctx));
      }
   }
}
