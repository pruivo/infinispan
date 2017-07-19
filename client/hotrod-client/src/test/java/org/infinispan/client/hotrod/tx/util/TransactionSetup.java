package org.infinispan.client.hotrod.tx.util;

import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.transaction.lookup.RemoteTransactionManagerLookup;
import org.infinispan.commons.util.LegacyKeySupportSystemProperties;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.test.fwk.JBossTransactionsUtils;
import org.infinispan.transaction.lookup.JBossStandaloneJTAManagerLookup;
import org.infinispan.transaction.lookup.TransactionManagerLookup;
import org.infinispan.util.tx.lookup.GeronimoTransactionManagerLookup;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public final class TransactionSetup {

   private static final String JBOSS_TM = "jbosstm";
   private static final String DUMMY_TM = "dummytm";
   private static final String GERONIMO_TM = "geronimotm";
   private static final String JTA = LegacyKeySupportSystemProperties
         .getProperty("infinispan.test.jta.tm", "infinispan.tm");
   private static TransactionManagerLookup lookup;

   static {
      // make the log in-memory to make tests run faster. Note that the config is frozen at system initialization time,
      // so you need to set this before classloading the transaction system and can't change it within the same vm.
      JBossTransactionsUtils.setVolatileStores();
   }

   static {
      init();
   }

   private TransactionSetup() {
   }

   private static void init() {
      String property = JTA;
      if (DUMMY_TM.equalsIgnoreCase(property)) {
         System.out.println("Transaction manager used: Dummy");
         lookup = RemoteTransactionManagerLookup.getInstance();
      } else if (GERONIMO_TM.equalsIgnoreCase(property)) {
         System.out.println("Transaction manager used: Geronimo");
         GeronimoTransactionManagerLookup tmLookup = new GeronimoTransactionManagerLookup();
         tmLookup.init(new GlobalConfigurationBuilder().build());
         lookup = tmLookup;
      } else {
         //use JBossTM as default (as in core)
         System.out.println("Transaction manager used: JBossTM");
         JBossStandaloneJTAManagerLookup tmLookup = new JBossStandaloneJTAManagerLookup();
         tmLookup.init(new GlobalConfigurationBuilder().build());
         lookup = tmLookup;
      }
   }

   public static void ammendJTA(ConfigurationBuilder builder) {
      builder.transaction().transactionManagerLookup(lookup);
   }

}
