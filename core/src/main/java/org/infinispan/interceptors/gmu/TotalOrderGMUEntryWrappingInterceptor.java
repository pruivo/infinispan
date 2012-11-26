package org.infinispan.interceptors.gmu;

import org.infinispan.commands.tx.GMUPrepareCommand;
import org.infinispan.context.impl.TxInvocationContext;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class TotalOrderGMUEntryWrappingInterceptor extends GMUEntryWrappingInterceptor {

   @Override
   protected void performValidation(TxInvocationContext ctx, GMUPrepareCommand command) throws InterruptedException {
      if (!ctx.isOriginLocal()) {
         //only in remote context the validation should be performed
         super.performValidation(ctx, command);
      }
   }
}
