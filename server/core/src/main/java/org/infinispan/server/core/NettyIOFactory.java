package org.infinispan.server.core;

import static org.infinispan.server.core.transport.NettyTransport.buildEventLoop;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.util.ProcessorInfo;
import org.infinispan.factories.AbstractComponentFactory;
import org.infinispan.factories.AutoInstantiableFactory;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.util.concurrent.BlockingTaskAwareExecutorServiceImpl;

import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import net.jcip.annotations.GuardedBy;

/**
 * //TODO document this!
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
@DefaultFactoryFor(names = {KnownComponentNames.NON_BLOCKING_EXECUTOR,  "org.infinispan.netty.io-threads", "org.infinispan.netty.io-master"})
public class NettyIOFactory extends AbstractComponentFactory implements AutoInstantiableFactory {

   //TODO put names somewhere
   public static final String IO_THREADS = "org.infinispan.netty.io-threads";
   public static final String IO_MASTER = "org.infinispan.netty.io-master";

   @GuardedBy("this")
   private EventLoopGroup ioThreads;

   @Override
   public Object construct(String componentName) {
      System.out.println("NETTY_IO_FACTORY: " + componentName);
      if (componentName.equals(KnownComponentNames.NON_BLOCKING_EXECUTOR)) {
         return new BlockingTaskAwareExecutorServiceImpl(getIoThreads(), globalComponentRegistry.getTimeService());
      } else if (componentName.equals(IO_THREADS)) {
         return getIoThreads();
      } else if (componentName.equals(IO_MASTER)) {
         //TODO thread name? or remove it, not sure if it can be re-used between "servers"
         return buildEventLoop(1, new DefaultThreadFactory("IO-ServerMaster"));
      } else {
         throw new CacheConfigurationException("Unknown named executor " + componentName);
      }
   }

   private synchronized EventLoopGroup getIoThreads() {
      if (ioThreads == null) {
         String nodeName = globalConfiguration.transport().nodeName();
         ioThreads = buildEventLoop(ProcessorInfo.availableProcessors(), new DefaultThreadFactory("non-blocking-thread-netty" + (nodeName != null ? "-" + nodeName : "")));
      }
      return ioThreads;
   }
}
