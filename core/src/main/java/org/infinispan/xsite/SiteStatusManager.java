package org.infinispan.xsite;

import static org.infinispan.util.logging.events.Messages.MESSAGES;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.transaction.Transaction;

import org.infinispan.Cache;
import org.infinispan.commands.AbstractVisitor;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.BackupFailurePolicy;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.BackupResponse;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.util.logging.events.EventLogCategory;
import org.infinispan.util.logging.events.EventLogManager;
import org.infinispan.util.logging.events.EventLogger;
import org.infinispan.xsite.notification.SiteStatusListener;

/**
 * @author Mircea Markus
 * @since 5.2
 */
public class SiteStatusManager {

   private static Log log = LogFactory.getLog(SiteStatusManager.class);
   private final Collection<XSiteBackup> syncSites = new CopyOnWriteArrayList<>();
   private final Collection<XSiteBackup> asyncSites = new CopyOnWriteArrayList<>();
   private final Map<String, SiteStatus> enabledSites = CollectionFactory.makeConcurrentMap();
   private Cache<Object, Object> cache;
   private TimeService timeService;
   private EventLogManager eventLogManager;
   private String localSiteName;
   private String cacheName;
   private GlobalConfiguration globalConfig;
   private Address localAddress;

   @Inject
   public void init(Cache<Object, Object> cache, Transport transport, TransactionTable txTable, GlobalConfiguration gc,
         TimeService timeService, CommandsFactory commandsFactory, EventLogManager eventLogManager) {
      this.cache = cache;
      this.globalConfig = gc;
      this.timeService = timeService;
      this.eventLogManager = eventLogManager;
   }

   @Start(priority = 11)
   public void start() {
      Configuration config = cache.getCacheConfiguration();
      this.cacheName = cache.getName();
      this.localSiteName = globalConfig.sites().localSite();
      this.localAddress = cache.getAdvancedCache().getRpcManager().getAddress();
      for (BackupConfiguration bc : config.sites().enabledBackups()) {
         final String siteName = bc.site();
         final BackupFailurePolicy backupFailurePolicy = bc.backupFailurePolicy();
         CustomFailurePolicy<Object, Object> customFailurePolicy = null;
         if (bc.backupFailurePolicy() == BackupFailurePolicy.CUSTOM) {
            String backupPolicy = bc.failurePolicyClass();
            if (backupPolicy == null) {
               throw new IllegalStateException("Backup policy class missing for custom failure policy!");
            }
            customFailurePolicy = Util.getInstance(backupPolicy, globalConfig.classLoader());
            customFailurePolicy.init(cache);
         }
         OfflineStatus offline = new OfflineStatus(bc.takeOffline(), timeService,
               new SiteStatusListener() {
                  @Override
                  public void siteOnline() {
                     SiteStatusManager.this.siteOnline(siteName);
                  }

                  @Override
                  public void siteOffline() {
                     SiteStatusManager.this.siteOffline(siteName);
                  }
               });
         enabledSites
               .put(siteName, new SiteStatus(backupFailurePolicy, customFailurePolicy, offline, bc.isTwoPhaseCommit()));
         if (bc.strategy() == BackupConfiguration.BackupStrategy.SYNC) {
            syncSites.add(new XSiteBackup(siteName, true, bc.replicationTimeout()));
         } else {
            asyncSites.add(new XSiteBackup(siteName, false, bc.replicationTimeout()));
         }
      }
   }

   public BackupSender.BringSiteOnlineResponse bringSiteOnline(String siteName) {
      SiteStatus siteStatus = enabledSites.get(siteName);
      if (siteStatus == null) {
         log.tryingToBringOnlineNonexistentSite(siteName);
         return BackupSender.BringSiteOnlineResponse.NO_SUCH_SITE;
      } else {
         return siteStatus.offlineStatus.bringOnline() ?
                BackupSender.BringSiteOnlineResponse.BROUGHT_ONLINE :
                BackupSender.BringSiteOnlineResponse.ALREADY_ONLINE;
      }
   }

   public BackupSender.TakeSiteOfflineResponse takeSiteOffline(String siteName) {
      SiteStatus siteStatus = enabledSites.get(siteName);
      if (siteStatus == null) {
         return BackupSender.TakeSiteOfflineResponse.NO_SUCH_SITE;
      } else {
         return siteStatus.offlineStatus.forceOffline() ?
                BackupSender.TakeSiteOfflineResponse.TAKEN_OFFLINE :
                BackupSender.TakeSiteOfflineResponse.ALREADY_OFFLINE;
      }
   }

   public void updateOfflineSites(BackupResponse backupResponse) {
      if (enabledSites.isEmpty() || backupResponse.isEmpty()) {
         return;
      }
      Set<String> communicationErrors = backupResponse.getCommunicationErrors();
      for (Map.Entry<String, SiteStatus> statusEntry : enabledSites.entrySet()) {
         OfflineStatus status = statusEntry.getValue().offlineStatus;
         if (!status.isEnabled()) {
            continue;
         }
         if (communicationErrors.contains(statusEntry.getKey())) {
            status.updateOnCommunicationFailure(backupResponse.getSendTimeMillis());
            log.tracef("OfflineStatus updated %s", status);
         } else if (!status.isOffline()) {
            status.reset();
         }
      }
   }

   public void processFailedResponses(BackupResponse backupResponse, VisitableCommand command, Transaction transaction)
         throws Throwable {
      Map<String, Throwable> failures = backupResponse.getFailedBackups();
      BackupFailureException backupException = null;
      for (Map.Entry<String, Throwable> failure : failures.entrySet()) {
         final String siteName = failure.getKey();
         SiteStatus siteStatus = enabledSites.get(siteName);
         if (siteStatus == null) {
            continue;
         }
         switch (siteStatus.backupFailurePolicy) {
            case FAIL:
               if (backupException == null) {
                  backupException = new BackupFailureException(cacheName);
               }
               backupException.addFailure(siteName, failure.getValue());
               break;
            case WARN:
               log.warnXsiteBackupFailed(cacheName, siteName, failure.getValue());
               break;
            case CUSTOM:
               command.acceptVisitor(null,
                     new CustomBackupPolicyInvoker(siteName, siteStatus.customFailurePolicy, transaction));
            default:
         }
      }
      if (backupException != null) {
         throw backupException;
      }
   }

   public List<XSiteBackup> calculateAsyncBackups() {
      List<XSiteBackup> backupInfo = new ArrayList<>(asyncSites.size());
      for (XSiteBackup backup : asyncSites) {
         String siteName = backup.getSiteName();
         if (siteName.equals(localSiteName)) {
            log.cacheBackupsDataToSameSite(localSiteName);
            continue;
         }
         if (isOffline(siteName)) {
            log.tracef("The site '%s' is offline, not backing up information to it", siteName);
            continue;
         }
         backupInfo.add(backup);
      }
      return backupInfo;
   }

   public List<XSiteBackup> calculateSyncBackups(BackupFilter backupFilter) {
      List<XSiteBackup> backupInfo = new ArrayList<>(syncSites.size());
      for (XSiteBackup backup : syncSites) {
         String siteName = backup.getSiteName();
         boolean twoPhaseCommit = enabledSites.get(siteName).twoPhaseCommit;
         if (siteName.equals(localSiteName)) {
            log.cacheBackupsDataToSameSite(localSiteName);
            continue;
         }
         if (backupFilter == BackupFilter.KEEP_1PC_ONLY && twoPhaseCommit) {
            continue;
         }
         if (backupFilter == BackupFilter.KEEP_2PC_ONLY && !twoPhaseCommit) {
            continue;
         }
         if (isOffline(siteName)) {
            log.tracef("The site '%s' is offline, not backing up information to it", siteName);
            continue;
         }
         backupInfo.add(backup);
      }
      return backupInfo;
   }

   public OfflineStatus getOfflineStatus(String site) {
      SiteStatus siteStatus = enabledSites.get(site);
      return siteStatus == null ? null : siteStatus.offlineStatus;
   }

   public Map<String, Boolean> status() {
      Map<String, Boolean> result = new HashMap<>(enabledSites.size());
      for (Map.Entry<String, SiteStatus> entry : enabledSites.entrySet()) {
         result.put(entry.getKey(), !entry.getValue().offlineStatus.isOffline());
      }
      return result;
   }

   public boolean hasAsyncSites() {
      return !asyncSites.isEmpty();
   }

   private boolean isOffline(String site) {
      OfflineStatus offline = getOfflineStatus(site);
      return offline != null && offline.isOffline();
   }

   private void siteOnline(String siteName) {
      getEventLogger().info(EventLogCategory.CLUSTER, MESSAGES.siteOnline(siteName));
   }

   private void siteOffline(String siteName) {
      getEventLogger().info(EventLogCategory.CLUSTER, MESSAGES.siteOffline(siteName));
   }

   private EventLogger getEventLogger() {
      return eventLogManager.getEventLogger().context(cacheName).scope(localAddress);
   }

   public enum BackupFilter {
      KEEP_1PC_ONLY,
      KEEP_2PC_ONLY,
      KEEP_ALL
   }

   private static final class CustomBackupPolicyInvoker extends AbstractVisitor {

      private final String site;
      private final CustomFailurePolicy<Object, Object> failurePolicy;
      private final Transaction tx;

      CustomBackupPolicyInvoker(String site, CustomFailurePolicy<Object, Object> failurePolicy, Transaction tx) {
         this.site = site;
         this.failurePolicy = failurePolicy;
         this.tx = tx;
      }

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
         failurePolicy.handlePutFailure(site, command.getKey(), command.getValue(), command.isPutIfAbsent());
         return null;
      }

      @Override
      public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
         failurePolicy.handleRemoveFailure(site, command.getKey(), command.getValue());
         return null;
      }

      @Override
      public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
         failurePolicy.handleReplaceFailure(site, command.getKey(), command.getOldValue(), command.getNewValue());
         return null;
      }

      @Override
      public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
         failurePolicy.handleClearFailure(site);
         return null;
      }

      @Override
      public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
         failurePolicy.handlePutAllFailure(site, command.getMap());
         return null;
      }

      @Override
      public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
         failurePolicy.handlePrepareFailure(site, tx);
         return null;
      }

      @Override
      public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
         failurePolicy.handleRollbackFailure(site, tx);
         return null;
      }

      @Override
      public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
         failurePolicy.handleCommitFailure(site, tx);
         return null;
      }

      @Override
      protected Object handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
         throw new IllegalStateException("Unknown command: " + command);
      }
   }

   private static class SiteStatus {
      private final BackupFailurePolicy backupFailurePolicy;
      private final CustomFailurePolicy<Object, Object> customFailurePolicy;
      private final OfflineStatus offlineStatus;
      private final boolean twoPhaseCommit;

      private SiteStatus(BackupFailurePolicy backupFailurePolicy,
            CustomFailurePolicy<Object, Object> customFailurePolicy, OfflineStatus offlineStatus,
            boolean twoPhaseCommit) {
         this.backupFailurePolicy = backupFailurePolicy;
         this.customFailurePolicy = customFailurePolicy;
         this.offlineStatus = offlineStatus;
         this.twoPhaseCommit = twoPhaseCommit;
      }
   }

}
