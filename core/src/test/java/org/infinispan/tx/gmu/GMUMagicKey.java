package org.infinispan.tx.gmu;

import org.infinispan.Cache;
import org.infinispan.distribution.MagicKey;

import java.util.Arrays;
import java.util.Random;

import static org.infinispan.distribution.DistributionTestHelper.addressOf;
import static org.infinispan.distribution.DistributionTestHelper.hasOwnersIgnoringOrder;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class GMUMagicKey extends MagicKey {

   public GMUMagicKey(String name, Cache<?, ?>... owners) {
      super();
      if (owners == null || owners.length == 0) {
         throw new IllegalArgumentException("Owners cannot be empty");
      }
      this.address = addressOf(owners[0]).toString();
      Random r = new Random();
      Object dummy;
      int attemptsLeft = 1000;
      do {
         // create a dummy object with this hashcode
         final int hc = r.nextInt();
         dummy = new Integer(hc);
         attemptsLeft--;

      } while (!hasOwnersIgnoringOrder(dummy, owners) && attemptsLeft >= 0);

      if (attemptsLeft < 0) {
         throw new IllegalStateException("Could not find any key owned by " + Arrays.toString(owners));
      }
      // we have found a hashcode that works!
      this.hashcode = dummy.hashCode();
      this.name = name;
   }
}
