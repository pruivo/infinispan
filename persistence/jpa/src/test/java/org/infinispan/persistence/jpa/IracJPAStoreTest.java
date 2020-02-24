package org.infinispan.persistence.jpa;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.jpa.configuration.JpaStoreConfigurationBuilder;
import org.infinispan.persistence.jpa.entity.KeyValueEntity;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.xsite.irac.persistence.BaseIracPersistenceTest;
import org.testng.annotations.Test;

/**
 * //TODO document this!
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
@Test(groups = "functional", testName = "persistence.jpa.IracJPAStoreTest")
public class IracJPAStoreTest extends BaseIracPersistenceTest<KeyValueEntity> {


   @Override
   protected KeyValueEntity wrap(String key, String value) {
      return new KeyValueEntity(key, value);
   }

   @Override
   protected String unwrap(KeyValueEntity valueEntity) {
      return valueEntity.getValue();
   }

   @Override
   protected SerializationContextInitializer getSerializationContextInitializer() {
      return JpaSCI.INSTANCE;
   }

   @Override
   protected void addPersistence(ConfigurationBuilder builder) {
      builder.persistence().addStore(JpaStoreConfigurationBuilder.class)
            .segmented(false)
            .persistenceUnitName("org.infinispan.persistence.jpa")
            .entityClass(KeyValueEntity.class);
   }
}
