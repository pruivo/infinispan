/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

package org.infinispan.container.versioning;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.context.Flag;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.Assert;
import org.testng.annotations.Test;

import javax.transaction.RollbackException;
import javax.transaction.Transaction;

import static org.testng.AssertJUnit.assertEquals;

@Test(testName = "container.versioning.ReplWriteSkewTest", groups = "functional")
@CleanupAfterMethod
public class ReplWriteSkewTest extends AbstractClusteredWriteSkewTest {

   @Override
   protected CacheMode getCacheMode() {
      return CacheMode.REPL_SYNC;
   }

   @Override
   protected int clusterSize() {
      return 2;
   }
   
   public void testWriteSkew() throws Exception {
      Cache<Object, Object> cache0 = cache(0);
      Cache<Object, Object> cache1 = cache(1);

      // Auto-commit is true
      cache0.put("hello", "world 1");
      assertEquals(cache0.get("hello"), "world 1");
      assertEventuallyEquals(1, "hello", "world 1");

      tm(0).begin();
      assert "world 1".equals(cache0.get("hello"));
      Transaction t = tm(0).suspend();

      // Induce a write skew
      cache1.put("hello", "world 3");

      assert cache0.get("hello").equals("world 3");
      assert cache1.get("hello").equals("world 3");

      tm(0).resume(t);
      cache0.put("hello", "world 2");

      try {
         tm(0).commit();
         assert false : "Transaction should roll back";
      } catch (RollbackException re) {
         // expected
      }

      assert "world 3".equals(cache0.get("hello"));
      assert "world 3".equals(cache1.get("hello"));

      assertNoTransactions();
   }

   public void testWriteSkewMultiEntries() throws Exception {
      Cache<Object, Object> cache0 = cache(0);
      Cache<Object, Object> cache1 = cache(1);

      tm(0).begin();
      cache0.put("hello", "world 1");
      cache0.put("hello2", "world 1");
      tm(0).commit();

      tm(0).begin();
      cache0.put("hello2", "world 2");
      assert "world 2".equals(cache0.get("hello2"));
      assert "world 1".equals(cache0.get("hello"));
      Transaction t = tm(0).suspend();

      // Induce a write skew
      // Auto-commit is true
      cache1.put("hello", "world 3");

      assert cache0.get("hello").equals("world 3");
      assert cache0.get("hello2").equals("world 1");
      assert cache1.get("hello").equals("world 3");
      assert cache1.get("hello2").equals("world 1");

      tm(0).resume(t);
      cache0.put("hello", "world 2");

      try {
         tm(0).commit();
         assert false : "Transaction should roll back";
      } catch (RollbackException re) {
         // expected
      }

      assert cache0.get("hello").equals("world 3");
      assert cache0.get("hello2").equals("world 1");
      assert cache1.get("hello").equals("world 3");
      assert cache1.get("hello2").equals("world 1");

      assertNoTransactions();
   }

   public void testNullEntries() throws Exception {

      // Auto-commit is true
      cache(0).put("hello", "world");

      assertEquals(cache(0).get("hello"), "world");
      assertEventuallyEquals(1, "hello", "world");


      tm(0).begin();
      assert "world".equals(cache(0).get("hello"));
      Transaction t = tm(0).suspend();

      cache(1).remove("hello");

      assertEventuallyEquals(0, "hello", null);
      assertEquals(cache(0).get("hello"), null);

      tm(0).resume(t);
      cache(0).put("hello", "world2");

      try {
         tm(0).commit();
         assert false : "This transaction should roll back";
      } catch (RollbackException expected) {
         // expected
      }

      assert null == cache(0).get("hello");
      assert null == cache(1).get("hello");

      log.tracef("Local tx for cache %s are ", 0, transactionTable(0).getLocalTransactions());
      log.tracef("Remote tx for cache %s are ", 0, transactionTable(0).getRemoteTransactions());
      log.tracef("Local tx for cache %s are ", 1, transactionTable(1).getLocalTransactions());
      log.tracef("Remote tx for cache %s are ", 0, transactionTable(1).getRemoteTransactions());

      assertNoTransactions();
   }

   public void testResendPrepare() throws Exception {
      Cache<Object, Object> cache0 = cache(0);
      Cache<Object, Object> cache1 = cache(1);

      // Auto-commit is true
      cache0.put("hello", "world");

      // create a write skew
      tm(0).begin();
      assert "world".equals(cache0.get("hello"));
      Transaction t = tm(0).suspend();

      // Implicit tx.  Prepare should be retried.
      cache(0).put("hello", "world2");

      assert "world2".equals(cache0.get("hello"));
      assertEventuallyEquals(1, "hello", "world2");

      tm(0).resume(t);
      cache0.put("hello", "world3");

      try {
         log.warn("----- Now committing ---- ");
         tm(0).commit();
         assert false : "This transaction should roll back";
      } catch (RollbackException expected) {
         // expected
         expected
               .printStackTrace();
      }

      assert "world2".equals(cache0.get("hello"));
      assert "world2".equals(cache1.get("hello"));

      assertNoTransactions();
   }

   public void testLocalOnlyPut() {
      localOnlyPut(this.<Integer, String>cache(0), 1, "v1");
      localOnlyPut(this.<Integer, String>cache(1), 2, "v2");
      assertNoTransactions();
   }

   public void testReplace() {      
      cache(1).put("key", "value1");

      assert "value1".equals(cache(1).get("key"));
      assert "value1".equals(cache(0).get("key"));

      Assert.assertEquals(cache(0).replace("key", "value2"), "value1");

      assert "value2".equals(cache(1).get("key"));
      assert "value2".equals(cache(0).get("key"));

      cache(0).put("key", "value3");

      cache(0).replace("key", "value3");

      assert "value3".equals(cache(1).get("key"));
      assert "value3".equals(cache(0).get("key"));

      assertNoTransactions();
   }

   public void testReplaceWithOldVal() {      
      cache(1).put("key", "value1");

      assert "value1".equals(cache(1).get("key"));
      assert "value1".equals(cache(0).get("key"));

      cache(0).put("key", "value2");

      assert "value2".equals(cache(1).get("key"));
      assert "value2".equals(cache(0).get("key"));

      assert !cache(0).replace("key", "value3", "value4");

      assert "value2".equals(cache(1).get("key"));
      assert "value2".equals(cache(0).get("key"));

      assert cache(0).replace("key", "value2", "value4");

      assert "value4".equals(cache(1).get("key"));
      assert "value4".equals(cache(0).get("key"));

      assertNoTransactions();
   }

   public void testRemoveUnexistingEntry() {      
      cache(0).remove("key");

      assert cache(1).get("key") == null;
      assert cache(0).get("key") == null;

      assertNoTransactions();
   }

   public void testRemoveIfPresent() {      

      cache(0).put("key", "value1");
      cache(1).put("key", "value2");

      assert "value2".equals(cache(1).get("key"));
      assert "value2".equals(cache(0).get("key"));

      cache(0).remove("key", "value1");

      assert "value2".equals(cache(1).get("key"));
      assert "value2".equals(cache(0).get("key"));

      cache(0).remove("key", "value2");

      assert cache(1).get("key") == null;
      assert cache(0).get("key") == null;

      assertNoTransactions();
   }


   @Test(enabled = false, description = "See ISPN-2160")
   @Override
   public void testSharedCounter() {
      super.testSharedCounter();
   }

   private void localOnlyPut(Cache<Integer, String> cache, Integer k, String v) {
      cache.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).put(k, v);
   }

}
