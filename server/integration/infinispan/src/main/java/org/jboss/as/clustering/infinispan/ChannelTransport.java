/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2011, Red Hat, Inc., and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.jboss.as.clustering.infinispan;

import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.transport.jgroups.CommandAwareRpcDispatcher;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.remoting.transport.jgroups.MarshallerAdapter;
import org.infinispan.server.jgroups.spi.ChannelFactory;
import org.jgroups.JChannel;
import org.jgroups.util.ByteArrayDataInputStream;

import java.io.DataInput;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;

/**
 * Custom {@link JGroupsTransport} that uses a provided channel.
 * @author Paul Ferraro
 */
public class ChannelTransport extends JGroupsTransport {

    final ChannelFactory factory;

    public ChannelTransport(JChannel channel, ChannelFactory factory) {
        super(channel);
        this.factory = factory;
    }

    @Override
    protected void initRPCDispatcher() {
        this.dispatcher = new CommandAwareRpcDispatcher(channel, this, globalHandler, this.getTimeoutExecutor(), timeService);
        Field bufField;
        try {
            bufField = ByteArrayDataInputStream.class.getDeclaredField("buf");
            bufField.setAccessible(true);
        } catch (NoSuchFieldException e) {
            throw new IllegalStateException("Field not found", e);
        }
        MarshallerAdapter adapter = new MarshallerAdapter(this.marshaller) {
            @Override
            public Object objectFromStream(DataInput in) throws Exception {
                byte[] buf = (byte[]) bufField.get(in);
                ByteArrayDataInputStream byteArrayDataInputStream = (ByteArrayDataInputStream) in;
                int offset = byteArrayDataInputStream.position();
                int length = byteArrayDataInputStream.limit() - offset;
                return ChannelTransport.this.factory
                             .isUnknownForkResponse(ByteBuffer.wrap(buf, offset, length)) ?
                       CacheNotFoundResponse.INSTANCE : super.objectFromStream(in);
            }
        };
        this.dispatcher.setMarshaller(adapter);
        this.dispatcher.start();
    }

    @Override
    protected synchronized void initChannel() {
        this.channel.setDiscardOwnMessages(false);
        this.connectChannel = true;
        this.disconnectChannel = true;
        this.closeChannel = false;
    }
}

