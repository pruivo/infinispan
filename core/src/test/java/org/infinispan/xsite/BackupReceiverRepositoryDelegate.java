package org.infinispan.xsite;

import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.xsite.statetransfer.XSiteStatePushCommand;
import org.infinispan.xsite.statetransfer.XSiteStateTransferControlCommand;
import org.jgroups.protocols.relay.SiteAddress;

/**
 * {@link org.infinispan.xsite.BackupReceiverRepository} delegate. Mean to be overridden. For test purpose only!
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public abstract class BackupReceiverRepositoryDelegate implements BackupReceiverRepository {

   protected final BackupReceiverRepository delegate;

   protected BackupReceiverRepositoryDelegate(BackupReceiverRepository delegate) {
      this.delegate = delegate;
   }

   @Override
   public Object handleRemoteCommand(SingleRpcCommand cmd, SiteAddress src) throws Throwable {
      return delegate.handleRemoteCommand(cmd, src);
   }

   @Override
   public void handleStateTransferControl(XSiteStateTransferControlCommand cmd, SiteAddress src) throws Exception {
      delegate.handleStateTransferControl(cmd, src);
   }

   @Override
   public void handleStateTransferState(XSiteStatePushCommand cmd, SiteAddress src) throws Exception {
      delegate.handleStateTransferState(cmd, src);
   }
}
