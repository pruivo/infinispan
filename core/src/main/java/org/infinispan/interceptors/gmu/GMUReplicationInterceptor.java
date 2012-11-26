package org.infinispan.interceptors.gmu;

import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.container.versioning.gmu.GMUVersionGenerator;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.ReplicationInterceptor;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Map;

import static org.infinispan.transaction.gmu.GMUHelper.joinAndSetTransactionVersion;
import static org.infinispan.transaction.gmu.GMUHelper.toGMUVersionGenerator;

/**
 * @author Pedro Ruivo
 * @author Sebastiano Peluso
 * @since 5.2
 */
public class GMUReplicationInterceptor extends ReplicationInterceptor {

   private static final Log log = LogFactory.getLog(GMUReplicationInterceptor.class);

   protected GMUVersionGenerator versionGenerator;

   @Inject
   public void inject(VersionGenerator versionGenerator){
      this.versionGenerator = toGMUVersionGenerator(versionGenerator);
   }

   @Override
   protected void broadcastPrepare(TxInvocationContext context, PrepareCommand command) {
      Map<Address, Response> responses = rpcManager.invokeRemotely(null, command, true, false, false);
      log.debugf("broadcast prepare command for transaction %s. responses are: %s",
                 command.getGlobalTransaction().prettyPrint(), responses.toString());

      joinAndSetTransactionVersion(responses.values(), context, versionGenerator);
   }
}
