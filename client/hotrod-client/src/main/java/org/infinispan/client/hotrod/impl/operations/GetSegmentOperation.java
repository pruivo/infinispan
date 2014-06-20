package org.infinispan.client.hotrod.impl.operations;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class GetSegmentOperation extends RetryOnFailureOperation<Map<byte[], byte[]>> {

   private final int segmentId;

   public GetSegmentOperation(Codec codec, TransportFactory transportFactory, byte[] cacheName,
                              AtomicInteger topologyId, Flag[] flags, int segmentId) {
      super(codec, transportFactory, cacheName, topologyId, flags);
      this.segmentId = segmentId;
   }


   @Override
   protected Transport getTransport(int retryCount, Set<SocketAddress> failedServers) {
      return transportFactory.getTransport(segmentId, failedServers);
   }

   @Override
   protected Map<byte[], byte[]> executeOperation(Transport transport) {
      HeaderParams params = writeHeader(transport, GET_SEGMENT_REQUEST);
      transport.writeVInt(segmentId);
      transport.flush();
      readHeaderAndValidate(transport, params);
      Map<byte[], byte[]> result = new HashMap<byte[], byte[]>();
      while ( transport.readByte() == 1) { //there's more!
         result.put(transport.readArray(), transport.readArray());
      }
      return result;
   }
}
