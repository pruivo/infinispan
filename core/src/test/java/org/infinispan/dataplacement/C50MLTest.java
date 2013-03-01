package org.infinispan.dataplacement;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.dataplacement.c50.C50MLObjectLookupFactory;
import org.infinispan.dataplacement.lookup.ObjectLookup;
import org.infinispan.test.AbstractCacheTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

/**
 * C5.0 Decision Tree test
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", testName = "dataplacement.C50MLTest")
public class C50MLTest extends AbstractCacheTest {

   private static final String C_50_ML_LOCATION = "/tmp/ml";
   private static final String BLOOM_FILTER_FALSE_POSITIVE_PROBABILITY = "0.001";
   private static final boolean SKIP_ML_RUNNING = true;
   private C50MLObjectLookupFactory objectLookupFactory;

   @BeforeClass
   public void setup() {
      ConfigurationBuilder configurationBuilder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      configurationBuilder.dataPlacement()
            .addProperty(C50MLObjectLookupFactory.KEY_FEATURE_MANAGER, DummyKeyFeatureManager.class.getCanonicalName())
            .addProperty(C50MLObjectLookupFactory.LOCATION, C_50_ML_LOCATION)
            .addProperty(C50MLObjectLookupFactory.BF_FALSE_POSITIVE, BLOOM_FILTER_FALSE_POSITIVE_PROBABILITY);
      objectLookupFactory = new C50MLObjectLookupFactory();
      objectLookupFactory.setConfiguration(configurationBuilder.build());
   }

   public void testMachineLearner() {
      if (SKIP_ML_RUNNING) {
         return;
      }

      for (int numberOfKeys = 1000; numberOfKeys < 10000; numberOfKeys *= 2) {
         for (int replicationDegree = 1; replicationDegree < 10; replicationDegree *= 2) {
            SegmentMapping segmentMapping = createRandomMovements(numberOfKeys, replicationDegree);
            //TODO FIX ME
            ObjectLookup objectLookup = objectLookupFactory.createObjectLookup(segmentMapping, replicationDegree);
            assert objectLookup != null;
            checkOwnerIndex(segmentMapping, objectLookup, "key:" + numberOfKeys + ",replication degree=" + replicationDegree);
         }
      }
   }

   private void checkOwnerIndex(SegmentMapping segmentMapping, ObjectLookup objectLookup, String test) {
      int errors = 0, wrongReplicationDegree = 0;
      long start, end, duration = 0;
      Iterator<SegmentMapping.KeyOwners> keyOwnersIterator = segmentMapping.iterator();
      while (keyOwnersIterator.hasNext()) {
         SegmentMapping.KeyOwners keyOwners = keyOwnersIterator.next();
         Set<Integer> expectedOwners = new TreeSet<Integer>();
         for (int index : keyOwners.getOwnerIndexes()) {
            expectedOwners.add(index);
         }
         start = System.currentTimeMillis();
         Collection<Integer> owners = objectLookup.query(keyOwners.getKey());
         end = System.currentTimeMillis();
         Set<Integer> ownersQuery = new TreeSet<Integer>(owners);

         wrongReplicationDegree += expectedOwners.size() == ownersQuery.size() ? 0 : 1;
         errors += expectedOwners.containsAll(ownersQuery) ? 0 : 1;
         duration += (end - start);
      }
      log.warnf("[%s], wrong keys moved: %s, wrong replication degree %s, total query duration: %s ms",
                test, errors, wrongReplicationDegree, duration);
   }

   private SegmentMapping createRandomMovements(int numberOfKeys, int replicationDegree) {
      Random random = new Random();
      Set<Object> keys = new HashSet<Object>();
      SegmentMapping segmentMapping = new SegmentMapping(1);
      while (keys.size() < numberOfKeys) {
         Object key;
         if (random.nextInt(100) < 50) {
            key = DummyKeyFeatureManager.getKey(random.nextInt(1000));
         } else {
            key = DummyKeyFeatureManager.getKey(random.nextInt(100), random.nextInt(1000));
         }
         OwnersInfo ownersInfo = new OwnersInfo(replicationDegree);
         Set<Integer> owners = new TreeSet<Integer>();
         while (owners.size() < replicationDegree) {
            owners.add(random.nextInt(replicationDegree * 3));
         }

         for (int ownerIndex : owners) {
            ownersInfo.add(ownerIndex, 0);
         }
         if (keys.add(key)) {
            segmentMapping.add(key, ownersInfo);
         }
      }

      return segmentMapping;
   }
}
