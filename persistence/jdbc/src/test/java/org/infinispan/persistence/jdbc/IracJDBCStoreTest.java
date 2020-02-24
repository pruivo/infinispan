package org.infinispan.persistence.jdbc;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.infinispan.xsite.irac.persistence.BaseIracPersistenceTest;
import org.testng.annotations.Test;

/**
 * //TODO document this!
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
@Test(groups = "functional", testName = "persistence.jdbc.IracJDBCStoreTest")
public class IracJDBCStoreTest extends BaseIracPersistenceTest<String> {


   @Override
   protected void addPersistence(ConfigurationBuilder builder) {
      JdbcStringBasedStoreConfigurationBuilder jdbcBuilder = builder.persistence()
            .addStore(JdbcStringBasedStoreConfigurationBuilder.class);
      UnitTestDatabaseManager.buildTableManipulation(jdbcBuilder.table());
      UnitTestDatabaseManager.configureUniqueConnectionFactory(jdbcBuilder);
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
