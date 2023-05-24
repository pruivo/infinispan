package org.infinispan.xsite.irac;

import java.util.Objects;
import java.util.concurrent.CompletionStage;

import net.jcip.annotations.GuardedBy;
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
   @GuardedBy("this")
   private ExponentialBackOff backOff;
   @GuardedBy("this")
   private boolean backOffEnabled;
   @GuardedBy("this")
   private Runnable afterBackoffRunnable = () -> {};

   public IracXSiteBackup(String siteName, boolean sync, long timeout, boolean logExceptions) {
      super(siteName, sync, timeout);
      this.logExceptions = logExceptions;
      this.backOffEnabled = false;
      this.backOff = ExponentialBackOff.NO_OP;
   }

   public boolean logExceptions() {
      return logExceptions;
   }

   public boolean isBackOffEnabled() {
      return backOffEnabled;
   }

   synchronized void useBackOff(ExponentialBackOff backOff, Runnable afterBackoffRunnable) {
      this.backOff = Objects.requireNonNull(backOff);
      this.afterBackoffRunnable = Objects.requireNonNull(afterBackoffRunnable);
   }

   synchronized void enableBackOff() {
      if (backOffEnabled) {
         return;
      }
      backOffEnabled = true;
      backOff.asyncBackOff().thenRun(this);
   }

   CompletionStage<Void> asyncBackOff() {
      return backOff.asyncBackOff();
   }

   synchronized void resetBackOff() {
      backOffEnabled = false;
      backOff.reset();
      afterBackoffRunnable.run();
   }

   @Override
   public synchronized void run() {
      backOffEnabled = false;
      afterBackoffRunnable.run();
   }

   @Override
   public String toString() {
      return super.toString() + (backOffEnabled ? " [backoff-enabled]" : "");
   }

   public static IracXSiteBackup fromBackupConfiguration(BackupConfiguration backupConfiguration) {
      return new IracXSiteBackup(backupConfiguration.site(), true, backupConfiguration.replicationTimeout(), backupConfiguration.backupFailurePolicy() == BackupFailurePolicy.WARN);
   }
}
