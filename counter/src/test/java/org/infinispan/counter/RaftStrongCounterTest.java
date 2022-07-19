package org.infinispan.counter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.counter.impl.CounterModuleLifecycle;
import org.infinispan.counter.impl.factory.RaftStrongCounterFactory;
import org.infinispan.counter.impl.factory.StrongCounterFactory;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.RaftUtil;
import org.infinispan.test.TestingUtil;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

/**
 * TODO!
 */
@Test(groups = "functional", testName = "counter.RaftStrongCounterTest")
public class RaftStrongCounterTest extends StrongCounterTest {

   private static final int CLUSTER_SIZE = 3;
   private static final List<String> RAFT_MEMBERS;

   static {
      List<String> members = new ArrayList<>(CLUSTER_SIZE);
      for (int i = 0; i < CLUSTER_SIZE; ++i) {
         members.add("RaftStrongCounterTest-" + i);
      }
      RAFT_MEMBERS = Collections.unmodifiableList(members);
   }

   @Override
   protected GlobalConfigurationBuilder configure(int nodeId) {
      GlobalConfigurationBuilder builder = super.configure(nodeId);
      builder.transport().raftMembers(RAFT_MEMBERS);
      builder.transport().nodeName("RaftStrongCounterTest-" + nodeId);
      return builder;
   }

   @Override
   protected int clusterSize() {
      return CLUSTER_SIZE;
   }

   @Override
   protected void afterCacheManagersCreated() {
      super.afterCacheManagersCreated();
      AssertJUnit.assertTrue(RaftUtil.isRaftAvailable());
      for (EmbeddedCacheManager cacheManager : cacheManagers) {
         AssertJUnit.assertTrue(TestingUtil.extractGlobalComponent(cacheManager, Transport.class).raftManager().isRaftAvailable());
         TestingUtil.replaceComponent(cacheManager, StrongCounterFactory.class, new RaftStrongCounterFactory(), true);
      }
      for (EmbeddedCacheManager cacheManager : cacheManagers) {
         eventually(() -> TestingUtil.extractGlobalComponent(cacheManager, Transport.class).raftManager().hasLeader(CounterModuleLifecycle.RAFT_COUNTER_CHANNEL_ID));
      }
   }
}
