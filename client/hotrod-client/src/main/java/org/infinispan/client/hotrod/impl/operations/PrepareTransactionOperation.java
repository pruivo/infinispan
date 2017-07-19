package org.infinispan.client.hotrod.impl.operations;

import java.net.SocketAddress;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import javax.transaction.xa.Xid;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.transaction.entry.Modification;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.client.hotrod.transaction.manager.RemoteXid;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public class PrepareTransactionOperation extends RetryOnFailureOperation<Integer> {

   private final Xid xid;
   private final boolean onePhaseCommit;
   private final Collection<Modification> modifications;
   private boolean retry;

   PrepareTransactionOperation(Codec codec, TransportFactory transportFactory, byte[] cacheName,
         AtomicInteger topologyId, Configuration cfg, Xid xid, boolean onePhaseCommit,
         Collection<Modification> modifications) {
      super(codec, transportFactory, cacheName, topologyId, 0, cfg);
      this.xid = xid;
      this.onePhaseCommit = onePhaseCommit;
      this.modifications = modifications;
   }

   public boolean shouldRetry() {
      return retry;
   }

   @Override
   protected Transport getTransport(int retryCount, Set<SocketAddress> failedServers) {
      return transportFactory.getTransport(failedServers, cacheName);
   }

   @Override
   protected Integer executeOperation(Transport transport) {
      retry = false;
      HeaderParams params = writeHeader(transport, PREPARE_REQUEST);
      writeXid(transport);
      transport.writeByte((short) (onePhaseCommit ? 1 : 0));
      transport.writeVInt(modifications.size());
      modifications.forEach(m -> m.writeTo(transport, codec));
      transport.flush();
      short status = readHeaderAndValidate(transport, params);
      if (status == NO_ERROR_STATUS) { //everything is "fine"
         return transport.read4ByteInt();
      }
      //prepare not executed. it is running on another node.
      retry = status == NOT_PUT_REMOVED_REPLACED_STATUS;
      return 0;
   }

   private void writeXid(Transport transport) {
      if (xid instanceof RemoteXid) {
         ((RemoteXid) xid).writeTo(transport);
      } else {
         transport.writeSignedVInt(xid.getFormatId());
         transport.writeArray(xid.getGlobalTransactionId());
         transport.writeArray(xid.getBranchQualifier());
      }
   }
}
