package org.infinispan.counter;

import java.lang.reflect.Method;

import org.infinispan.counter.impl.factory.JGroupsCounterFactory;
import org.infinispan.counter.impl.factory.StrongCounterFactory;
import org.infinispan.counter.impl.jgroups.UnboundedJGroupsStrongCounter;
import org.infinispan.counter.util.StrongTestCounter;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.test.TestingUtil;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

/**
 * TODO! document this
 */
@Test(groups = "functional", testName = "counter.JGroupsStrongCounterTest")
public class JGroupsStrongCounterTest extends StrongCounterTest {

   @Override
   protected void afterClusterCreated() {
      cacheManagers.forEach(JGroupsStrongCounterTest::replaceStrongCounterFactory);
   }

   public void testCorrectCounter(Method method) {
      StrongTestCounter testCounter = createCounter(counterManager(0), method.getName(), 0);
      testCounter.assertImplementation(UnboundedJGroupsStrongCounter.class);
   }

   private static void replaceStrongCounterFactory(EmbeddedCacheManager cacheManager) {
      Transport transport = TestingUtil.extractGlobalComponent(cacheManager, Transport.class);
      AssertJUnit.assertTrue(transport instanceof JGroupsTransport);
      // nothing to stop in cache based factory
      TestingUtil.replaceComponent(cacheManager, StrongCounterFactory.class, JGroupsCounterFactory.create((JGroupsTransport) transport), true);
   }
}
