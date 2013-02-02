/*
 * INESC-ID, Instituto de Engenharia de Sistemas e Computadores Investigação e Desevolvimento em Lisboa
 * Copyright 2013 INESC-ID and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 3.0 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
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
