package org.infinispan.tx.totalorder;

import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.TransactionProtocol;
import org.infinispan.transaction.totalorder.SequentialTotalOrderManager;
import org.infinispan.transaction.totalorder.TotalOrderManager;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * if sync enlistment is used then always use 1PC as the prepare is not relevant.
 *
 * @author Mircea Markus <mircea.markus@jboss.com> (C) 2011 Red Hat Inc.
 * @since 5.2
 */
@Test (groups = "functional", testName = "tx.totaloreder.ForceOnePhaseWhenSyncEnlistmentTest")
public class ForceOnePhaseWhenSyncEnlistmentTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder dcc = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      dcc.transaction().transactionProtocol(TransactionProtocol.TOTAL_ORDER)
            .useSynchronization(true)
            .recovery().disable();
      dcc.locking().isolationLevel(IsolationLevel.REPEATABLE_READ).writeSkewCheck(false)
            .versioning().enable().scheme(VersioningScheme.SIMPLE);
      createCluster(dcc, 2);
      waitForClusterToForm();
   }

   public void testNoTwoPhaseInterceptors() {
      TotalOrderManager component = advancedCache(0).getComponentRegistry().getComponent(TotalOrderManager.class);
      assert component instanceof SequentialTotalOrderManager;
      MyCommandInterceptor ci = new MyCommandInterceptor();
      advancedCache(1).addInterceptor(ci, 1);
      cache(0).put("k","v");
      assertEquals(cache(0).get("k"),"v");
      assertEventuallyEquals(1, "k", "v");
      assert !ci.commitReceived;
      assert ci.isOnePhase;
   }

   static class MyCommandInterceptor extends CommandInterceptor {

      volatile boolean isOnePhase;
      volatile boolean commitReceived;

      @Override
      public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
         isOnePhase = command.isOnePhaseCommit();
         return super.visitPrepareCommand(ctx, command);
      }

      @Override
      public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
         commitReceived = true;
         return super.visitCommitCommand(ctx, command);
      }
   }
}
