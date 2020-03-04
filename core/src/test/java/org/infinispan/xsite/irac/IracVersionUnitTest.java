package org.infinispan.xsite.irac;

import org.infinispan.container.versioning.InequalVersionComparisonResult;
import org.infinispan.container.versioning.irac.DefaultIracVersionGenerator;
import org.infinispan.container.versioning.irac.IracEntryVersion;
import org.infinispan.container.versioning.irac.TopologyIracVersion;
import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.notifications.cachelistener.event.TopologyChangedEvent;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.mockito.Mockito;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 10.1
 */
@Test(groups = "unit", testName = "xsite.irac.IracVersionUnitTest")
public class IracVersionUnitTest extends AbstractInfinispanTest {

   private static final String SITE_1 = "site_1";
   private static final String SITE_2 = "site_2";

   private static final CacheNotifier<?, ?> MOCK_CACHE_NOTIFIER;

   static {
      MOCK_CACHE_NOTIFIER = Mockito.mock(CacheNotifier.class);
      Mockito.doNothing().when(MOCK_CACHE_NOTIFIER).addListener(Mockito.any());
      Mockito.doNothing().when(MOCK_CACHE_NOTIFIER).removeListener(Mockito.any());
   }

   private static DefaultIracVersionGenerator newGenerator(String site) {
      Transport transport = mockTransport(site);
      DefaultIracVersionGenerator generator = new DefaultIracVersionGenerator();
      TestingUtil.inject(generator, transport, MOCK_CACHE_NOTIFIER);
      generator.start();
      triggerTopologyEvent(generator, 1);
      return generator;
   }

   private static Transport mockTransport(String siteName) {
      Transport t = Mockito.mock(Transport.class);
      Mockito.doNothing().when(t).checkCrossSiteAvailable();
      Mockito.when(t.localSiteName()).thenReturn(siteName);
      return t;
   }

   private static TopologyChangedEvent<?, ?> mockTopologyEvent(int topologyId) {
      TopologyChangedEvent<?, ?> t = Mockito.mock(TopologyChangedEvent.class);
      Mockito.when(t.getNewTopologyId()).thenReturn(topologyId);
      return t;
   }

   private static void assertSiteVersion(IracEntryVersion entryVersion, String site, int topologyId, long version) {
      AssertJUnit.assertNotNull(entryVersion);
      TopologyIracVersion iracVersion = entryVersion.toMap().get(site);
      AssertJUnit.assertNotNull(iracVersion);
      AssertJUnit.assertEquals(topologyId, iracVersion.getTopologyId());
      AssertJUnit.assertEquals(version, iracVersion.getVersion());
   }

   private static void assertNoSiteVersion(IracEntryVersion entryVersion, String site) {
      AssertJUnit.assertNotNull(entryVersion);
      TopologyIracVersion iracVersion = entryVersion.toMap().get(site);
      AssertJUnit.assertNull(iracVersion);
   }

   private static void triggerTopologyEvent(DefaultIracVersionGenerator generator, int topologyId) {
      generator.onTopologyChange(mockTopologyEvent(topologyId));
   }

   public void testEquals() {
      IracMetadata m1 = newGenerator(SITE_1).generateNewMetadata(0);
      IracMetadata m2 = newGenerator(SITE_1).generateNewMetadata(0);

      AssertJUnit.assertEquals(InequalVersionComparisonResult.EQUAL, m1.getVersion().compareTo(m2.getVersion()));
      AssertJUnit.assertEquals(InequalVersionComparisonResult.EQUAL, m2.getVersion().compareTo(m1.getVersion()));
   }

   public void testCompareDifferentTopology() {
      DefaultIracVersionGenerator g1 = newGenerator(SITE_1);

      IracMetadata m1 = g1.generateNewMetadata(0); // (1,0)
      triggerTopologyEvent(g1, 2);

      IracMetadata m2 = g1.generateNewMetadata(0); //(1+,0)

      assertSiteVersion(m1.getVersion(), SITE_1, 1, 1);
      assertNoSiteVersion(m1.getVersion(), SITE_2);

      assertSiteVersion(m2.getVersion(), SITE_1, 2, 1);
      assertNoSiteVersion(m2.getVersion(), SITE_2);

      // we have m1=(1,0) and m2=(1+,0)
      AssertJUnit.assertEquals(InequalVersionComparisonResult.BEFORE, m1.getVersion().compareTo(m2.getVersion()));
      AssertJUnit.assertEquals(InequalVersionComparisonResult.AFTER, m2.getVersion().compareTo(m1.getVersion()));
   }

   public void testCompareSameTopology() {
      DefaultIracVersionGenerator g1 = newGenerator(SITE_1);

      IracMetadata m1 = g1.generateNewMetadata(0); // (1,0)
      IracMetadata m2 = g1.generateNewMetadata(0); // (2,0)

      assertSiteVersion(m1.getVersion(), SITE_1, 1, 1);
      assertNoSiteVersion(m1.getVersion(), SITE_2);

      assertSiteVersion(m2.getVersion(), SITE_1, 1, 2);
      assertNoSiteVersion(m2.getVersion(), SITE_2);

      AssertJUnit.assertEquals(InequalVersionComparisonResult.BEFORE, m1.getVersion().compareTo(m2.getVersion()));
      AssertJUnit.assertEquals(InequalVersionComparisonResult.AFTER, m2.getVersion().compareTo(m1.getVersion()));
   }

   public void testCausality() {
      DefaultIracVersionGenerator g1 = newGenerator(SITE_1);
      DefaultIracVersionGenerator g2 = newGenerator(SITE_2);

      IracMetadata m2 = g2.generateNewMetadata(0); // (0,1)

      assertNoSiteVersion(m2.getVersion(), SITE_1);
      assertSiteVersion(m2.getVersion(), SITE_2, 1, 1);


      g1.updateVersion(0, m2.getVersion());
      IracMetadata m1 = g1.generateNewMetadata(0); // (1,1)

      assertSiteVersion(m1.getVersion(), SITE_1, 1, 1);
      assertSiteVersion(m1.getVersion(), SITE_2, 1, 1);

      //we have m1=(1,1) and m2=(0,1)
      AssertJUnit.assertEquals(InequalVersionComparisonResult.BEFORE, m2.getVersion().compareTo(m1.getVersion()));
      AssertJUnit.assertEquals(InequalVersionComparisonResult.AFTER, m1.getVersion().compareTo(m2.getVersion()));
   }

   public void testConflictSameTopology() {
      DefaultIracVersionGenerator g1 = newGenerator(SITE_1);
      DefaultIracVersionGenerator g2 = newGenerator(SITE_2);

      IracMetadata m1 = g1.generateNewMetadata(0); // (1,0)
      IracMetadata m2 = g2.generateNewMetadata(0); // (0,1)

      AssertJUnit.assertEquals(SITE_1, m1.getSite());
      AssertJUnit.assertEquals(SITE_2, m2.getSite());

      assertSiteVersion(m1.getVersion(), SITE_1, 1, 1);
      assertNoSiteVersion(m1.getVersion(), SITE_2);

      assertNoSiteVersion(m2.getVersion(), SITE_1);
      assertSiteVersion(m2.getVersion(), SITE_2, 1, 1);

      //we have a conflict: (1,0) vs (0,1)

      AssertJUnit.assertEquals(InequalVersionComparisonResult.CONFLICTING, m1.getVersion().compareTo(m2.getVersion()));
   }

   public void testConflictDifferentTopology() {
      DefaultIracVersionGenerator g1 = newGenerator(SITE_1);
      DefaultIracVersionGenerator g2 = newGenerator(SITE_2);

      g2.generateNewMetadata(0); //(0,1)
      IracMetadata m2 = g2.generateNewMetadata(0); //(0,2)

      g1.updateVersion(0, m2.getVersion());
      IracMetadata m1 = g1.generateNewMetadata(0); //(1,2)

      triggerTopologyEvent(g2, 2);
      m2 = g2.generateNewMetadata(0); //(0,1+)

      //we should have a conflict m1=(1,2) & m2=(0,1+)

      assertSiteVersion(m1.getVersion(), SITE_1, 1, 1);
      assertSiteVersion(m1.getVersion(), SITE_2, 1, 2);

      assertNoSiteVersion(m2.getVersion(), SITE_1);
      assertSiteVersion(m2.getVersion(), SITE_2, 2, 1);

      AssertJUnit.assertEquals(InequalVersionComparisonResult.CONFLICTING, m1.getVersion().compareTo(m2.getVersion()));
      AssertJUnit.assertEquals(InequalVersionComparisonResult.CONFLICTING, m2.getVersion().compareTo(m1.getVersion()));
   }

   public void testNoConflictDifferentTopology() {
      DefaultIracVersionGenerator g1 = newGenerator(SITE_1);
      DefaultIracVersionGenerator g2 = newGenerator(SITE_2);

      IracMetadata m3 = g2.generateNewMetadata(0); //(0,1)
      IracMetadata m2 = g2.generateNewMetadata(0); //(0,2)

      g1.updateVersion(0, m2.getVersion());
      IracMetadata m1 = g1.generateNewMetadata(0); //(1,2)

      //we have m1=(1,2) & m3=(0,1).

      assertSiteVersion(m1.getVersion(), SITE_1, 1, 1);
      assertSiteVersion(m1.getVersion(), SITE_2, 1, 2);

      assertNoSiteVersion(m3.getVersion(), SITE_1);
      assertSiteVersion(m3.getVersion(), SITE_2, 1, 1);

      AssertJUnit.assertEquals(InequalVersionComparisonResult.AFTER, m1.getVersion().compareTo(m3.getVersion()));
      AssertJUnit.assertEquals(InequalVersionComparisonResult.BEFORE, m3.getVersion().compareTo(m1.getVersion()));
   }


}
