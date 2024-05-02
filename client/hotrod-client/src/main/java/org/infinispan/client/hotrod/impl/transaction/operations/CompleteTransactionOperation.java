package org.infinispan.client.hotrod.impl.transaction.operations;

import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;
import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.ClientTopology;
import org.infinispan.client.hotrod.impl.operations.RetryOnFailureOperation;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;
import org.infinispan.client.hotrod.telemetry.impl.TelemetryService;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import jakarta.transaction.TransactionManager;

/**
 * Represents a commit or rollback request from the {@link TransactionManager}.
 *
 * @author Pedro Ruivo
 * @since 9.3
 */
public class CompleteTransactionOperation extends RetryOnFailureOperation<Integer> {

   private final Xid xid;

   public CompleteTransactionOperation(Codec codec, ChannelFactory channelFactory, AtomicReference<ClientTopology> clientTopology,
                                       Configuration cfg, Xid xid, boolean commit, TelemetryService telemetryService) {
      super(commit ? COMMIT_REQUEST : ROLLBACK_REQUEST, commit ? COMMIT_RESPONSE : ROLLBACK_RESPONSE,
            codec, channelFactory, DEFAULT_CACHE_NAME_BYTES, clientTopology, 0, cfg, null, telemetryService);
      this.xid = xid;
   }

   @Override
   protected void executeOperation(Channel channel) {
      scheduleRead(channel);
      ByteBuf buf = channel.alloc().buffer(estimateSize());
      codec.writeHeader(buf, header);
      ByteBufUtil.writeXid(buf, xid);
      channel.writeAndFlush(buf);
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      if (status == NO_ERROR_STATUS) {
         complete(buf.readInt());
      } else {
         complete(XAException.XA_HEURRB);
      }
   }

   private int estimateSize() {
      return codec.estimateHeaderSize(header) + ByteBufUtil.estimateXidSize(xid);
   }

}
