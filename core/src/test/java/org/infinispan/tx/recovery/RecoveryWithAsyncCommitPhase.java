/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.tx.recovery;

import org.infinispan.Cache;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.remote.recovery.TxCompletionNotificationCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.remoting.InboundInvocationHandler;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.CommandAwareRpcDispatcher;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;

import static org.infinispan.test.TestingUtil.extractComponent;
import static org.infinispan.test.TestingUtil.replaceComponent;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
@Test(groups = "functional", testName = "tx.recovery.RecoveryWithAsyncCommitPhase")
public class RecoveryWithAsyncCommitPhase extends MultipleCacheManagersTest {

   private static final int NUM_NODES = 2;
   private static final String KEY = "key";
   private static final String VALUE = "value1";
   private static final String CACHE_NAME = "_test_";

   public void testAsyncCommitPhase() {
      CommandAwareInvocationHandler handler = replaceInvocationHandler(cache(1));
      //forces the commit to be deliver after the tx completion command.
      OnCommand commitAndTxCompletionSync = new OnCommand() {

         private boolean txCompletionReceived = false;

         @Override
         public synchronized void beforeCommand(CacheRpcCommand command) {
            if (command instanceof CommitCommand && CACHE_NAME.equals(command.getCacheName())) {
               while (!txCompletionReceived) {
                  try {
                     wait();
                  } catch (InterruptedException e) {
                     Thread.currentThread().interrupt();
                     return;
                  }
               }
               TestingUtil.sleepThread(1000);
            }
         }

         @Override
         public synchronized void afterCommand(CacheRpcCommand command) {
            if (command instanceof TxCompletionNotificationCommand && CACHE_NAME.equals(command.getCacheName())) {
               this.txCompletionReceived = true;
               notifyAll();
            }
         }
      };
      handler.add(commitAndTxCompletionSync);
      Assert.assertNull(cache(0, CACHE_NAME).get(KEY));
      Assert.assertNull(cache(1, CACHE_NAME).get(KEY));

      cache(0, CACHE_NAME).put(KEY, VALUE);
      Assert.assertEquals(cache(0, CACHE_NAME).get(KEY), VALUE);
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return VALUE.equals(cache(1, CACHE_NAME).get(KEY));
         }
      });
      assertNoTransactions();
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      builder.transaction().syncCommitPhase(false).useSynchronization(false);
      builder.transaction().recovery().enable();
      createClusteredCaches(NUM_NODES, builder);
      waitForClusterToForm(CACHE_NAME);
   }

   private CommandAwareInvocationHandler replaceInvocationHandler(Cache<?, ?> cache) {
      CommandAwareInvocationHandler newHandler = new CommandAwareInvocationHandler(extractComponent(cache, InboundInvocationHandler.class));
      replaceComponent(cache.getCacheManager(), InboundInvocationHandler.class, newHandler, true);
      JGroupsTransport t = (JGroupsTransport) extractComponent(cache, Transport.class);
      CommandAwareRpcDispatcher card = t.getCommandAwareRpcDispatcher();
      Field f;
      try {
         f = card.getClass().getDeclaredField("inboundInvocationHandler");
         f.setAccessible(true);
         f.set(card, newHandler);
      } catch (NoSuchFieldException e) {
         Assert.fail(e.getMessage());
      } catch (IllegalAccessException e) {
         Assert.fail(e.getMessage());
      }
      return newHandler;
   }

   public interface OnCommand {

      void beforeCommand(CacheRpcCommand command);

      void afterCommand(CacheRpcCommand command);
   }

   public class CommandAwareInvocationHandler implements InboundInvocationHandler {

      public final ArrayList<OnCommand> listeners;
      private final InboundInvocationHandler actual;

      public CommandAwareInvocationHandler(InboundInvocationHandler actual) {
         this.actual = actual;
         this.listeners = new ArrayList<OnCommand>(2);
      }

      public final void add(OnCommand onCommand) {
         listeners.add(onCommand);
      }

      public final void remove(OnCommand onCommand) {
         listeners.remove(onCommand);
      }

      @Override
      public Response handle(CacheRpcCommand command, Address origin) throws Throwable {
         for (OnCommand onCommand : listeners) {
            onCommand.beforeCommand(command);
         }
         Response response = actual.handle(command, origin);
         for (OnCommand onCommand : listeners) {
            onCommand.afterCommand(command);
         }
         return response;
      }
   }
}
