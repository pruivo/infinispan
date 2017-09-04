package org.infinispan.client.hotrod.tx;

import org.infinispan.client.hotrod.configuration.TransactionMode;
import org.infinispan.client.hotrod.impl.transaction.TransactionalRemoteCache;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.client.hotrod.tx.util.KeyValueGenerator;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
@Test(groups = "functional", testName = "client.hotrod.tx.APITxTest")
public class APITxTest<K, V> extends MultiHotRodServersTest {

   private static final int NR_NODES = 3;
   private static final String CACHE_NAME = "api-tx-cache";

   private KeyValueGenerator<K, V> kvGenerator;
   private TransactionMode transactionMode;

   @Override
   public Object[] factory() {
      return new Object[]{
            new APITxTest<String, String>()
                  .keyValueGenerator(KeyValueGenerator.STRING_GENERATOR).transactionMode(TransactionMode.NON_XA),
            new APITxTest<byte[], byte[]>()
                  .keyValueGenerator(KeyValueGenerator.BYTE_ARRAY_GENERATOR).transactionMode(TransactionMode.NON_XA),
            new APITxTest<Object[], Object[]>()
                  .keyValueGenerator(KeyValueGenerator.GENERIC_ARRAY_GENERATOR).transactionMode(TransactionMode.NON_XA)
      };
   }

   private APITxTest<K, V> transactionMode(TransactionMode transactionMode) {
      this.transactionMode = transactionMode;
      return this;
   }

   private APITxTest<K, V> keyValueGenerator(KeyValueGenerator<K, V> kvGenerator) {
      this.kvGenerator = kvGenerator;
      return this;
   }

   @Override
   protected String[] parameterNames() {
      return concat(super.parameterNames(), null, null);
   }

   @Override
   protected Object[] parameterValues() {
      return concat(super.parameterValues(), kvGenerator.toString(), transactionMode);
   }

   @Override
   protected String parameters() {
      return "[" + kvGenerator + "/" + transactionMode + "]";
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cacheBuilder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      cacheBuilder.transaction().lockingMode(LockingMode.PESSIMISTIC);
      cacheBuilder.locking().isolationLevel(IsolationLevel.REPEATABLE_READ);
      createHotRodServers(NR_NODES, new ConfigurationBuilder());
      defineInAll(CACHE_NAME, cacheBuilder);
   }

   public void testGet() {
      TransactionalRemoteCache<K, V> txRemoteCache = txRemoteCache(0);
      
   }

   private TransactionalRemoteCache<K, V> txRemoteCache(int index) {
      return (TransactionalRemoteCache<K, V>) client(index).getTransactionalCache(CACHE_NAME);
   }


}
