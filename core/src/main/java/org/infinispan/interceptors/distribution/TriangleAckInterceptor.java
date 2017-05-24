package org.infinispan.interceptors.distribution;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.util.concurrent.CommandAckCollector;
import org.infinispan.util.concurrent.CompletableFutures;

/**
 * It handles the acknowledges for the triangle algorithm (distributed mode only!)
 * <p>
 * It is placed between the {@link org.infinispan.statetransfer.StateTransferInterceptor} and the {@link
 * org.infinispan.interceptors.locking.NonTransactionalLockingInterceptor}.
 * <p>
 * The acknowledges are sent after the lock is released and it interacts with the {@link
 * org.infinispan.statetransfer.StateTransferInterceptor} to trigger the retries when topology changes while the
 * commands are processing.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class TriangleAckInterceptor extends DDAsyncInterceptor {

   private CommandAckCollector commandAckCollector;

   @Inject
   public void inject(CommandAckCollector commandAckCollector) {
      this.commandAckCollector = commandAckCollector;
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return ctx.isOriginLocal() ? invokeNextAndHandle(ctx, command, this::onWriteCommand) : invokeNext(ctx, command);
   }

   private Object onWriteCommand(InvocationContext rCtx, VisitableCommand rCommand, Object rv, Throwable t)
         throws Throwable {
      final DataWriteCommand cmd = (DataWriteCommand) rCommand;
      if (rCtx.isOriginLocal()) {
         Collector<Object> collector = commandAckCollector.getCollector(cmd.getCommandInvocationId());
         if (collector == null) {
            CompletableFutures.rethrowException(t);
            return rv;
         }
         if (t != null) {
            collector.primaryException(t);
         }

         return asyncValue(collector.getFuture());
      } else {
         return rv;
      }
   }

}
