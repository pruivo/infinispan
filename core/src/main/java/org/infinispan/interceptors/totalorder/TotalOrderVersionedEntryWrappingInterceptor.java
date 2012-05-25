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
