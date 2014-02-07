package org.infinispan.xsite;

import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.xsite.statetransfer.XSiteStatePushCommand;
import org.infinispan.xsite.statetransfer.XSiteStateTransferControlCommand;
import org.jgroups.protocols.relay.SiteAddress;

/**
 * Global component that holds all the {@link BackupReceiver}s within this CacheManager.
 *
 * @author Mircea Markus
 * @since 5.2
 */
@Scope(Scopes.GLOBAL)
public interface BackupReceiverRepository {

   /**
    * Process an CacheRpcCommand received from a remote site.
    */
   public Object handleRemoteCommand(SingleRpcCommand cmd, SiteAddress src) throws Throwable;

   /**
    * It handles the state transfer control from a remote site. The control command must be broadcast to the entire
    * cluster in which the cache exists.
    */
   public void handleStateTransferControl(XSiteStateTransferControlCommand cmd, SiteAddress src) throws Exception;

   /**
    * It handles the state transfer state from a remote site. It is possible to have a single node applying the state or
    * forward the state to respective primary owners.
    */
   public void handleStateTransferState(XSiteStatePushCommand cmd, SiteAddress src) throws Exception;
}
