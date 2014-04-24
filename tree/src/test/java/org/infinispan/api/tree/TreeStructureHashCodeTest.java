package org.infinispan.api.tree;

import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.tree.Fqn;
import org.infinispan.tree.impl.NodeKey;
import org.infinispan.util.concurrent.locks.containers.LockContainer;
import org.infinispan.util.concurrent.locks.containers.ReentrantStripedLockContainer;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests the degree to which hash codes get spread
 */
@Test(groups = "unit", testName = "api.tree.TreeStructureHashCodeTest")
public class TreeStructureHashCodeTest {

   public void testHashCodesAppendedCount() {
      List<Fqn> fqns = new ArrayList<Fqn>();
      fqns.add(Fqn.ROOT);
      for (int i = 0; i < 256; i++) fqns.add(Fqn.fromString("/fqn" + i));
      doTest(fqns);
   }

   public void testHashCodesAlpha() {
      List<Fqn> fqns = new ArrayList<Fqn>();
      fqns.add(Fqn.ROOT);
      for (int i = 0; i < 256; i++) fqns.add(Fqn.fromString("/" + Integer.toString(i, 36)));
      doTest(fqns);
   }

   private void doTest(List<Fqn> fqns) {
      LockContainer container = new ReentrantStripedLockContainer(512, AnyEquivalence.getInstance());
      Map<Integer, Integer> distribution = new HashMap<Integer, Integer>();
      for (Fqn f : fqns) {
         NodeKey dataKey = new NodeKey(f, NodeKey.Type.DATA);
         NodeKey structureKey = new NodeKey(f, NodeKey.Type.STRUCTURE);
         addToDistribution(container.getLockId(dataKey), distribution);
         addToDistribution(container.getLockId(structureKey), distribution);
      }

      System.out.println("Distribution: " + distribution);
      assert distribution.size() <= container.size() : "Cannot have more locks than the container size!";
      // assume at least a 2/3rd even distribution
      // but also consider that data snd structure keys would typically provide the same hash code
      // so we need to double this
      assert distribution.size() * 1.5 * 2 >= container.size() : "Poorly distributed!  Distribution size is just " + distribution.size() + " and there are " + container.size() + " shared locks";

   }

   private void addToDistribution(int lock, Map<Integer, Integer> map) {
      int count = 1;
      if (map.containsKey(lock)) count = map.get(lock) + 1;
      map.put(lock, count);
   }
}
