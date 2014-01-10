package org.infinispan.interceptors.xsite;

import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.remoting.transport.BackupResponse;

/**
 * Handles x-site data backups for non-transactional caches. The main goal is to wait if the backup is configured as
 * Synchronous
 *
 * @author Mircea Markus
 * @author Pedro Ruivo
 * @since 7.0
 */
public class NonTransactionalBackupResponseInterceptor extends BaseBackupInterceptor {

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return invokeNextAndAwaitResponse(ctx, command);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return invokeNextAndAwaitResponse(ctx, command);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return invokeNextAndAwaitResponse(ctx, command);
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      return invokeNextAndAwaitResponse(ctx, command);
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      return invokeNextAndAwaitResponse(ctx, command);
   }

   /**
    * waits until the backup is completed
    */
   private Object invokeNextAndAwaitResponse(InvocationContext context, WriteCommand command) throws Throwable {
      try {
         Object result = invokeNextInterceptor(context, command);
         BackupResponse backupResponse = context.getBackupResponse();
         if (backupResponse != null) {
            backupSender.processResponses(backupResponse, command);
         }
         return result;
      } finally {
         context.setBackupResponse(null);
      }
   }
}
