package org.infinispan.lock;

import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.util.concurrent.locks.impl.InfinispanLock;
import org.infinispan.util.concurrent.locks.impl.StripedLockContainer;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Test(groups = "unit", testName = "lock.LockContainerHashingTest")
public class LockContainerHashingTest extends AbstractInfinispanTest {
   private StripedLockContainer stripedLock;

   @BeforeMethod
   public void setUp() {
      stripedLock = new StripedLockContainer(500, AnyEquivalence.getInstance());
      stripedLock.inject(TIME_SERVICE);
   }

   public void testHashingDistribution() {
      // ensure even bucket distribution of lock stripes
      List<String> keys = createRandomKeys(1000);

      Map<InfinispanLock, Integer> distribution = new HashMap<>();

      for (String s : keys) {
         InfinispanLock lock = stripedLock.getLock(s);
         log.tracef("Lock for %s is %s", s, lock);
         if (distribution.containsKey(lock)) {
            int count = distribution.get(lock) + 1;
            distribution.put(lock, count);
         } else {
            distribution.put(lock, 1);
         }
      }

      // cannot be larger than the number of locks
      log.trace("dist size: " + distribution.size());
      log.trace("num shared locks: " + stripedLock.size());
      assert distribution.size() <= stripedLock.size();
      // assume at least a 2/3rd spread
      assert distribution.size() * 1.5 >= stripedLock.size();
   }

   private List<String> createRandomKeys(int number) {

      List<String> f = new ArrayList<>(number);
      int i = number;
      while (f.size() < number) {
         String s = i + "baseKey" + (10000 + i++);
         f.add(s);
      }

      return f;
   }
}
