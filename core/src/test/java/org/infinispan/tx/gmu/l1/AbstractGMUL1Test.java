package org.infinispan.tx.gmu.l1;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.gmu.L1GMUContainer;
import org.infinispan.tx.gmu.AbstractGMUTest;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public abstract class AbstractGMUL1Test extends AbstractGMUTest {

   @Override
   protected void decorate(ConfigurationBuilder builder) {
      builder.clustering().l1().enable().enableOnRehash();
   }

   @Override
   protected final CacheMode cacheMode() {
      return CacheMode.DIST_SYNC;
   }

   protected final void assertInL1Cache(int cacheIndex, Object... keys) {
      assert keys != null : "Cannot check if null keys is in cache";
      L1GMUContainer l1GMUContainer = getComponent(cacheIndex, L1GMUContainer.class);
      assert l1GMUContainer != null : "L1 GMU Container is null";
      for (Object key : keys) {
         assert l1GMUContainer.contains(key) : "Key " + key + " not in L1 GMU container of " + cacheIndex;
      }
   }

   protected final void printL1Container() {
      if (log.isDebugEnabled()) {
         StringBuilder stringBuilder = new StringBuilder("\n\n====== L1 Container ======\n");
         for (int i = 0; i < cacheManagers.size(); ++i) {
            L1GMUContainer l1GMUContainer = getComponent(i, L1GMUContainer.class);
            assert l1GMUContainer != null : "L1 GMU Container is null";
            stringBuilder.append(l1GMUContainer.chainToString())
                  .append("\n")
                  .append("===================\n");
         }
         log.debugf(stringBuilder.toString());
      }
   }
}
