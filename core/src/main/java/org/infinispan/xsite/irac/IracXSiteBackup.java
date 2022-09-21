package org.infinispan.xsite.irac;

import java.util.concurrent.CompletionStage;

import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.BackupFailurePolicy;
import org.infinispan.util.ExponentialBackOff;
import org.infinispan.xsite.XSiteBackup;

/**
 * Extends {@link XSiteBackup} class with logging configuration.
 *
 * @since 14.0
 */
public class IracXSiteBackup extends XSiteBackup implements Runnable {

   private final boolean logExceptions;
   private ExponentialBackOff backOff;
   private volatile boolean backOffMode;

   public IracXSiteBackup(String siteName, boolean sync, long timeout, boolean logExceptions) {
      super(siteName, sync, timeout);
      this.logExceptions = logExceptions;
      this.backOffMode = false;
   }

   public boolean logExceptions() {
      return logExceptions;
   }

   public void enableBackOffMode() {
      this.backOffMode = true;
   }

   public void disableBackOffMode() {
      this.backOffMode = false;
   }

   public boolean isBackOffMode() {
      return backOffMode;
   }

   @Override
   public String toString() {
      return super.toString() + (backOffMode ? " [backoff-enabled]" : "");
   }

   public static IracXSiteBackup fromBackupConfiguration(BackupConfiguration backupConfiguration) {
      return new IracXSiteBackup(backupConfiguration.site(), true, backupConfiguration.replicationTimeout(), backupConfiguration.backupFailurePolicy() == BackupFailurePolicy.WARN);
   }

   void useBackOff(ExponentialBackOff backOff) {
      this.backOff = backOff;
   }

   CompletionStage<Void> asyncBackOff() {
      return backOff.asyncBackOff();
   }

   void resetBackOff() {
      disableBackOffMode();
      backOff.reset();
   }

   @Override
   public void run() {
      disableBackOffMode();
   }
}
