package org.infinispan.persistence.rocksdb;

import java.io.File;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.rocksdb.configuration.RocksDBStoreConfigurationBuilder;
import org.infinispan.xsite.irac.persistence.BaseIracPersistenceTest;
import org.testng.annotations.Test;

/**
 * //TODO document this!
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
@Test(groups = "functional", testName = "persistence.rocksdb.IracRocksDBStoreTest")
public class IracRocksDBStoreTest extends BaseIracPersistenceTest<String> {


   @Override
   protected void addPersistence(ConfigurationBuilder builder) {
      builder.persistence().addStore(RocksDBStoreConfigurationBuilder.class)
            .location(tmpDirectory + File.separator + "data")
            .expiredLocation(tmpDirectory + File.separator + "expiry");
   }

   @Override
   protected String wrap(String key, String value) {
      return value;
   }

   @Override
   protected String unwrap(String value) {
      return value;
   }
}
