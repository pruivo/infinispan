package org.infinispan.xsite.irac.persistence;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 10.1
 */
@Test(groups = "functional", testName = "xsite.irac.persistence.IracSingleFileStoreTest")
public class IracSingleFileStoreTest extends BaseIracPersistenceTest<String> {


   @Override
   protected void addPersistence(ConfigurationBuilder builder) {
      builder.persistence().addSingleFileStore().location(tmpDirectory);
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
