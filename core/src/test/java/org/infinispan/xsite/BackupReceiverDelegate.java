package org.infinispan.xsite;

import org.infinispan.Cache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.xsite.statetransfer.XSiteStatePushCommand;
import org.infinispan.xsite.statetransfer.XSiteStateTransferControlCommand;

/**
 * {@link org.infinispan.xsite.BackupReceiver} delegate. Mean to be overridden. For test purpose only!
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public abstract class BackupReceiverDelegate implements BackupReceiver {

   protected final BackupReceiver delegate;

   protected BackupReceiverDelegate(BackupReceiver delegate) {
      if (delegate == null) {
         throw new NullPointerException("Delegate cannot be null");
      }
      this.delegate = delegate;
   }

   @Override
   public Cache getCache() {
      return delegate.getCache();
   }

   @Override
   public Object handleRemoteCommand(VisitableCommand command) throws Throwable {
      return delegate.handleRemoteCommand(command);
   }

   @Override
   public void handleStateTransferControl(XSiteStateTransferControlCommand command) throws Exception {
      delegate.handleStateTransferControl(command);
   }

   @Override
   public void handleStateTransferState(XSiteStatePushCommand cmd) throws Exception {
      delegate.handleStateTransferState(cmd);
   }
}
