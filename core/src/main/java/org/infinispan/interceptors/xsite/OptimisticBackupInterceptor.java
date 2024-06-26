package org.infinispan.interceptors.xsite;

import java.util.function.Predicate;

import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.InvocationStage;

/**
 * Handles x-site data backups for optimistic transactional caches.
 *
 * @author Mircea Markus
 * @since 5.2
 */
public class OptimisticBackupInterceptor extends BaseBackupInterceptor {

   private boolean hasOnePhaseCommitBackups;

   @Inject
   public void checkTwoPhaseCommit(Configuration configuration) {
      hasOnePhaseCommitBackups = configuration.sites().syncBackupsStream().anyMatch(Predicate.not(BackupConfiguration::isTwoPhaseCommit));
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) {
      if (skipXSiteBackup(command) || !command.hasAnyFlag(FlagBitSets.PUT_FOR_EXTERNAL_READ)) {
         return invokeNext(ctx, command);
      }
      return invokeNextThenApply(ctx, command, handleSingleKeyWriteReturn);
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) {
      //if this is a read-only tx or state transfer tx, no point replicating it to other sites
      if (!shouldInvokeRemoteTxCommand(ctx) || isTxFromRemoteSite(command.getGlobalTransaction())) {
         return invokeNext(ctx, command);
      }

      InvocationStage stage = backupSender.backupPrepare(command, ctx.getCacheTransaction(), ctx.getTransaction());
      return invokeNextAndWaitForCrossSite(ctx, command, stage);
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) {
      if (isTxFromRemoteSite(command.getGlobalTransaction())) {
         return invokeNext(ctx, command);
      }

      InvocationStage stage;
      if (shouldInvokeRemoteTxCommand(ctx)) { // this assumes sync.
         // for sync, the originator is the one who backups the transaction to the remote site.
         // we can send the update to the remote site in parallel with the local cluster update.
         stage = backupSender.backupCommit(command, ctx.getTransaction());
      } else {
         //for async, all nodes need to keep track of the updates keys.
         stage = InvocationStage.completedNullStage();
      }

      if (hasOnePhaseCommitBackups) {
         // One or more backup site(s) using one phase commit.
         // Wait for backup site acknowledge, so we have time to rollback and keep the data consistent.
         return makeStage(asyncInvokeNext(ctx, command, stage))
               .thenApply(ctx, command, (rCtx, rCommand, rv) -> {
                  //for async, all nodes need to keep track of the updates keys after it is applied locally.
                  trackKeysForAsyncBackups(rCommand);
                  return rv;
               });
      }

      // The backup site(s) is using 2PC, we can send the commit in parallel for both local and remote site.
      return invokeNextThenApply(ctx, command, (rCtx, rCommand, rv) -> {
         //we need to track the keys only after it is applied in the local node!
         trackKeysForAsyncBackups(rCommand);
         return stage.thenReturn(rCtx, rCommand, rv);
      });
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) {
      if (!shouldRollbackRemoteTxCommand(ctx))
         return invokeNext(ctx, command);

      if (isTxFromRemoteSite(command.getGlobalTransaction())) {
         return invokeNext(ctx, command);
      }

      InvocationStage stage = backupSender.backupRollback(command, ctx.getTransaction());
      return invokeNextAndWaitForCrossSite(ctx, command, stage);
   }

   private boolean shouldRollbackRemoteTxCommand(TxInvocationContext<?> ctx) {
      return shouldInvokeRemoteTxCommand(ctx) && hasBeenPrepared((LocalTxInvocationContext) ctx);
   }

   /**
    * This 'has been prepared' logic only applies to optimistic transactions, hence it is not present in the
    * LocalTransaction object itself.
    */
   private boolean hasBeenPrepared(LocalTxInvocationContext ctx) {
      return !ctx.getRemoteLocksAcquired().isEmpty();
   }

   private void trackKeysForAsyncBackups(CommitCommand command) {
      var gtx = command.getGlobalTransaction();
      keysFromMods(getModificationsFrom(command))
            .forEach(key -> iracManager.trackUpdatedKey(key.getSegment(), key.getKey(), gtx));
   }
}
