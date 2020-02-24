package org.infinispan.persistence.sifs;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.sifs.configuration.SoftIndexFileStoreConfigurationBuilder;
import org.infinispan.xsite.irac.persistence.BaseIracPersistenceTest;
import org.testng.annotations.Test;

/**
 * //TODO document this!
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
@Test(groups = "functional", testName = "persistence.sifs.IracMetadataSIFSStoreTest")
public class IracSIFSStoreTest extends BaseIracPersistenceTest<String> {

   @Override
   protected void addPersistence(ConfigurationBuilder builder) {
      builder.persistence().addStore(SoftIndexFileStoreConfigurationBuilder.class)
            .dataLocation(tmpDirectory)
            .indexLocation(tmpDirectory);
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
