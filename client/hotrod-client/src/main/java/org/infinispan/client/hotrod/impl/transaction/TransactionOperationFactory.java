package org.infinispan.client.hotrod.impl.transaction;

import javax.transaction.xa.Xid;
import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.ClientTopology;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transaction.operations.CompleteTransactionOperation;
import org.infinispan.client.hotrod.impl.transaction.operations.ForgetTransactionOperation;
import org.infinispan.client.hotrod.impl.transaction.operations.RecoveryOperation;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.telemetry.impl.TelemetryService;
import org.infinispan.client.hotrod.telemetry.impl.TelemetryServiceFactory;

/**
 * An operation factory that builds operations independent from the cache used.
 * <p>
 * This operations are the commit/rollback request, forget request and in-doubt transactions request.
 * <p>
 * This operation aren't associated to any cache, but they use the default cache topology to pick the server to
 * contact.
 *
 * @author Pedro Ruivo
 * @since 10.0
 */
public class TransactionOperationFactory {

   private final Configuration configuration;
   private final ChannelFactory channelFactory;
   private final AtomicReference<ClientTopology> clientTopology;
   private final TelemetryService telemetryService;

   public TransactionOperationFactory(Configuration configuration, ChannelFactory channelFactory) {
      this.configuration = configuration;
      this.channelFactory = channelFactory;
      clientTopology = channelFactory.createTopologyId(HotRodConstants.DEFAULT_CACHE_NAME_BYTES);
      telemetryService = TelemetryServiceFactory.INSTANCE.telemetryService(configuration.tracingPropagationEnabled());
   }

   CompleteTransactionOperation newCompleteTransactionOperation(Xid xid, boolean commit) {
      return new CompleteTransactionOperation(channelFactory.getNegotiatedCodec(), channelFactory, clientTopology, configuration, xid, commit, telemetryService);
   }

   ForgetTransactionOperation newForgetTransactionOperation(Xid xid) {
      return new ForgetTransactionOperation(channelFactory.getNegotiatedCodec(), channelFactory, clientTopology, configuration, xid, telemetryService);
   }

   RecoveryOperation newRecoveryOperation() {
      return new RecoveryOperation(channelFactory.getNegotiatedCodec(), channelFactory, clientTopology, configuration, telemetryService);
   }

}
