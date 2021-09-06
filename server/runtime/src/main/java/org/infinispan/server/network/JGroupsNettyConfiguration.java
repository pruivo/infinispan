package org.infinispan.server.network;

import static org.infinispan.server.core.transport.NettyTransport.buildEventLoop;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.remoting.transport.jgroups.JGroupsProtocolVisitor;
import org.infinispan.server.core.transport.NettyTransport;
import org.infinispan.server.logging.Log;
import org.jgroups.protocols.netty.Netty;
import org.jgroups.stack.Protocol;
import org.kohsuke.MetaInfServices;

import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.EventExecutorGroup;
import netty.configuration.NettyEventLoopSupplier;

@MetaInfServices(value = JGroupsProtocolVisitor.class)
public class JGroupsNettyConfiguration implements JGroupsProtocolVisitor, NettyEventLoopSupplier {

   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass(), Log.class);

   private GlobalComponentRegistry registry;
   private EventLoopGroup parentGroup;

   @Override
   public void init(boolean useNative) {
      this.parentGroup = buildEventLoop(1, new DefaultThreadFactory("JGroups-ServerMaster"), "JGroups");
   }

   @Override
   public EventLoopGroup getParentGroup() {
      return parentGroup;
   }

   @Override
   public EventLoopGroup getChildGroup() {
      // non-blocking executor
      return registry.getComponent(EventLoopGroup.class);
   }

   @Override
   public EventExecutorGroup getWorkerGroup() {
      // non-blocking executor
      return registry.getComponent(EventLoopGroup.class);
   }

   @Override
   public void shutdownGracefully() throws InterruptedException {
      parentGroup.shutdownGracefully(100, 1000, TimeUnit.MILLISECONDS).sync();
   }

   @Override
   public void inject(GlobalComponentRegistry registry) {
      this.registry = registry;
   }

   @Override
   public void configureProtocol(Protocol protocol) {
      if (!(protocol instanceof Netty)) {
         return;
      }
      log.warn("Configuring Netty Protocol");
      Netty netty = (Netty) protocol;
      netty.setEventLoopSupplier(this);
      netty.setUse_native_transport(NettyTransport.isNativeAvailable());
   }
}
