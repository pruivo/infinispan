package org.infinispan.counter;

import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.counter.api.CounterType;
import org.infinispan.counter.api.Storage;
import org.infinispan.counter.impl.factory.RaftJGroupsCounterFactory;
import org.infinispan.counter.impl.factory.StrongCounterFactory;
import org.infinispan.counter.impl.jgroups.UnboundedJGroupsStrongCounter;
import org.infinispan.counter.util.StrongTestCounter;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.JGroupsRaftManager;
import org.infinispan.remoting.transport.raft.RaftManager;
import org.infinispan.test.TestingUtil;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * TODO! document this
 */
@Test(groups = "functional", testName = "counter.RaftStrongCounterTest")
public class RaftStrongCounterTest extends StrongCounterTest {

   private static final int CLUSTER_SIZE = 4;
   private static final List<String> RAFT_MEMBERS;
   private static final String tmpDirectory = CommonsTestingUtil.tmpDirectory(RaftStrongCounterTest.class);

   static {
      List<String> members = new ArrayList<>(CLUSTER_SIZE);
      for (int i = 0; i < CLUSTER_SIZE; ++i) {
         // must contain the class name or the testsuite will fail
         members.add(RaftStrongCounterTest.class.getSimpleName() + i);
      }
      RAFT_MEMBERS = Collections.unmodifiableList(members);
   }

   @BeforeClass(alwaysRun = true)
   @Override
   public void createBeforeClass() throws Throwable {
      Util.recursiveFileRemove(tmpDirectory);
      super.createBeforeClass();
   }

   @AfterClass(alwaysRun = true)
   @Override
   protected void destroy() {
      super.destroy();
      Util.recursiveFileRemove(tmpDirectory);
   }

   @Override
   protected void afterClusterCreated() {
      cacheManagers.forEach(RaftStrongCounterTest::replaceStrongCounterFactory);
   }

   @Override
   protected GlobalConfigurationBuilder configure(int nodeId) {
      String raftMember = RAFT_MEMBERS.get(nodeId);
      GlobalConfigurationBuilder builder = super.configure(nodeId);
      builder.globalState().enable().persistentLocation(Path.of(tmpDirectory, raftMember).toString());
      // enable RAFT by configuring the node's name and raft-members
      builder.transport()
            .raftMembers(RAFT_MEMBERS)
            .nodeName(raftMember);
      return builder;
   }

   @Override
   protected int clusterSize() {
      return CLUSTER_SIZE;
   }

   @Override
   protected List<CounterConfiguration> configurationToTest() {
      return Arrays.asList(
            CounterConfiguration.builder(CounterType.UNBOUNDED_STRONG).storage(Storage.PERSISTENT).initialValue(10).build(),
            CounterConfiguration.builder(CounterType.UNBOUNDED_STRONG).storage(Storage.PERSISTENT).initialValue(20).build(),
            CounterConfiguration.builder(CounterType.UNBOUNDED_STRONG).storage(Storage.PERSISTENT).build()
      );
   }

   @Override
   protected StrongTestCounter createCounter(CounterManager counterManager, String counterName, long initialValue) {
      CounterConfiguration config = CounterConfiguration.builder(CounterType.UNBOUNDED_STRONG)
            .storage(Storage.PERSISTENT)
            .initialValue(initialValue)
            .build();
      return createCounter(counterManager, counterName, config);
   }

   public void testCorrectCounter(Method method) {
      StrongTestCounter testCounter = createCounter(counterManager(0), method.getName(), 0);
      testCounter.assertImplementation(UnboundedJGroupsStrongCounter.class);
   }

   private static void replaceStrongCounterFactory(EmbeddedCacheManager cacheManager) {
      Transport transport = TestingUtil.extractGlobalComponent(cacheManager, Transport.class);
      RaftManager raftManager = transport.raftManager();
      AssertJUnit.assertTrue(raftManager.isRaftAvailable() && raftManager instanceof JGroupsRaftManager);
      // nothing to stop in cache based factory
      TestingUtil.replaceComponent(cacheManager, StrongCounterFactory.class, RaftJGroupsCounterFactory.create((JGroupsRaftManager) raftManager), true);
   }
}
