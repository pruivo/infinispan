package org.infinispan.client.hotrod.impl.operations;

import java.net.SocketAddress;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.client.hotrod.transaction.manager.RemoteXid;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public class CompleteTransactionOperation extends RetryOnFailureOperation<Integer> {

   private final Xid xid;
   private final byte requestId;

   protected CompleteTransactionOperation(Codec codec, TransportFactory transportFactory, byte[] cacheName,
         AtomicInteger topologyId, Configuration cfg, Xid xid, boolean commit) {
      super(codec, transportFactory, cacheName, topologyId, 0, cfg);
      this.xid = xid;
      this.requestId = commit ? COMMIT_REQUEST : ROLLBACK_REQUEST;
   }

   @Override
   protected Transport getTransport(int retryCount, Set<SocketAddress> failedServers) {
      return transportFactory.getTransport(failedServers, cacheName);
   }

   @Override
   protected Integer executeOperation(Transport transport) {
      HeaderParams params = writeHeader(transport, requestId);
      writeXid(transport);
      transport.flush();
      short status = readHeaderAndValidate(transport, params);
      //everything is "fine"
      return status == NO_ERROR_STATUS ? transport.read4ByteInt() : XAResource.XA_OK;
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
