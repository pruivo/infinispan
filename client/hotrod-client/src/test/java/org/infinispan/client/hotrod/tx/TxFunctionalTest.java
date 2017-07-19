package org.infinispan.client.hotrod.tx;

import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;

import java.lang.reflect.Method;

import javax.transaction.TransactionManager;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.impl.transaction.TransactionalRemoteCache;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.client.hotrod.tx.util.TransactionSetup;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
@Test(groups = "functional", testName = "client.hotrod.tx.TxFunctionalTest")
public class TxFunctionalTest extends MultiHotRodServersTest {

   public TxFunctionalTest() {
      transactional = true;
      cacheMode = CacheMode.DIST_SYNC;
      isolationLevel = IsolationLevel.REPEATABLE_READ;
   }

   public void testSimpleTransaction(Method method) throws Exception {
      final String k1 = k(method, "k1");
      final String k2 = k(method, "k2");
      final String v1 = v(method, "v1");
      final String v2 = v(method, "v2");

      TransactionalRemoteCache<String, String> remoteCache = remoteCache(0);
      final TransactionManager tm = remoteCache.getTransactionManager();

      //test with tx
      tm.begin();
      remoteCache.put(k1, v1);
      remoteCache.put(k2, v1);
      tm.commit();

      assertEntryInAllClients(k1, v1);
      assertEntryInAllClients(k2, v1);

      //test without tx
      remoteCache.put(k1, v2);
      remoteCache.put(k2, v2);

      assertEntryInAllClients(k1, v2);
      assertEntryInAllClients(k2, v2);
   }

   protected final <K, V> TransactionalRemoteCache<K, V> remoteCache(int index) {
      return (TransactionalRemoteCache<K, V>) client(index).getTransactionalCache(cacheName());
   }

   protected int numberOfNodes() {
      return 3;
   }

   protected String cacheName() {
      return "tx-cache";
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      org.infinispan.configuration.cache.ConfigurationBuilder cacheBuilder = getDefaultClusteredCacheConfig(cacheMode,
            transactional);
      cacheBuilder.transaction().transactionManagerLookup(new EmbeddedTransactionManagerLookup());
      cacheBuilder.transaction().lockingMode(lockingMode);
      cacheBuilder.locking().isolationLevel(isolationLevel);
      createHotRodServers(numberOfNodes(), new ConfigurationBuilder());
      defineInAll(cacheName(), cacheBuilder);
   }

   @Override
   protected org.infinispan.client.hotrod.configuration.ConfigurationBuilder createHotRodClientConfigurationBuilder(
         int serverPort) {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder = super
            .createHotRodClientConfigurationBuilder(serverPort);
      TransactionSetup.ammendJTA(clientBuilder);
      return clientBuilder;
   }

   private <K, V> void assertEntryInAllClients(K key, V value) {
      for (RemoteCacheManager manager : clients) {
         RemoteCache<K, V> remoteCache = manager.getTransactionalCache(cacheName());
         AssertJUnit.assertEquals(value, remoteCache.get(key));
      }
   }

}
