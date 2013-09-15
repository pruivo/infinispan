package org.infinispan.reconfigurableprotocol;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", testName = "reconfigurableprotocol.WriteSkewMorphRTest")
@CleanupAfterMethod
public class WriteSkewMorphRTest extends MorphRTest {


   @Override
   protected void decorate(ConfigurationBuilder builder) {
      super.decorate(builder);
      builder.locking()
            .isolationLevel(IsolationLevel.REPEATABLE_READ)
            .writeSkewCheck(true);
      builder.versioning()
            .enable()
            .scheme(VersioningScheme.SIMPLE);
   }
}
