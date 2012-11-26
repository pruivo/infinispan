package org.infinispan.tx.gmu.totalorder;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.transaction.TransactionProtocol;
import org.infinispan.tx.gmu.DistSimpleTest;
import org.infinispan.tx.gmu.SimpleTest;
import org.testng.annotations.Test;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", testName = "tx.gmu.totalorder.TotalOrderDistSimpleTest")
public class TotalOrderDistSimpleTest extends DistSimpleTest {

   @Override
   protected void decorate(ConfigurationBuilder builder) {
      super.decorate(builder);
      builder.transaction().transactionProtocol(TransactionProtocol.TOTAL_ORDER);
   }
}
