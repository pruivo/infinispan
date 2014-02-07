package org.infinispan.xsite.statetransfer;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@Test(groups = "xsite", testName = "xsite.stateTransfer.DistSyncTwoPhasesTxStateTransferTest")
public class DistSyncTwoPhasesTxStateTransferTest extends BaseStateTransferTest {

   public DistSyncTwoPhasesTxStateTransferTest() {
      super();
      use2Pc = true;
      implicitBackupCache = true;
   }

   @Override
   protected ConfigurationBuilder getNycActiveConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
   }

   @Override
   protected ConfigurationBuilder getLonActiveConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
   }

   @Test(enabled = false)
   @Override
   public void testClearOperationBeforeState() throws Exception {
      /*
        It does not work with the current clear implementation. this is what is happening:
        0) assume cache is not empty
        1) state transfer and clear happens at the same time
        2) clear (PrepareCommand) arrives first than the state
        2.1) clear is replayed. the behavior is to iterate over all keys, lock, wrap, and mark them as removed.
             however, there is no key in the cache
        3) the state arrives. it acquires locks and applies the keys (note that 2.1) didn't acquire any lock)
        4) CommitCommand arrives. For all entries wrapped, it is removed. However, nothing was wrapped so nothing gets
           removed

        Possible solutions:
        * global lock (problem: negative impact in performance since all operations must acquire the global lock)
        * make clear invoke datacontainer.clear() (problem: no locks acquired)
        * block operations in the remote site until all state is received (problem: bad... really bad...)

        isn't happen in NBST?
       */
      super.testClearOperationBeforeState();
   }
}
