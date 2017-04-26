package org.infinispan.xsite.async;

import static org.infinispan.persistence.PersistenceUtil.loadAndStoreInDataContainer;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.impl.AbstractInvocationContext;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.remoting.transport.BackupResponse;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.SiteStatusManager;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public class AsyncXSiteManager {

   private static final Log log = LogFactory.getLog(AsyncXSiteManager.class);

   private final Map<Object, Object> modifiedKeys;
   private Transport transport;
   private ClusteringDependentLogic clusteringDependentLogic;
   private DataContainer<Object, Object> dataContainer;
   private PersistenceManager persistenceManager;
   private TimeService timeService;
   private CommandsFactory commandsFactory;
   private int batchSize = 10; //TODO configure
   private volatile LastIteration lastIteration;
   private SiteStatusManager siteStatusManager;
   private ScheduledExecutorService executorService;

   public AsyncXSiteManager() {
      modifiedKeys = CollectionFactory.makeConcurrentMap();
   }

   @Inject
   public void inject(Transport transport, ClusteringDependentLogic clusteringDependentLogic,
         DataContainer<Object, Object> dataContainer, PersistenceManager persistenceManager, TimeService timeService,
         CommandsFactory commandsFactory, SiteStatusManager siteStatusManager) {
      this.transport = transport;
      this.clusteringDependentLogic = clusteringDependentLogic;
      this.dataContainer = dataContainer;
      this.persistenceManager = persistenceManager;
      this.timeService = timeService;
      this.commandsFactory = commandsFactory;
      this.siteStatusManager = siteStatusManager;
   }

   @Start(priority = 12)
   public void start() {
      executorService = Executors.newScheduledThreadPool(1);
      executorService.scheduleWithFixedDelay(this::backupRemotelyIteration, 10, 10, TimeUnit.SECONDS);
   }

   public void removeKey(Object key, Object lastWriter) {
      modifiedKeys.remove(key, lastWriter);
   }

   void backupPrepare(PrepareCommand command, Collection<WriteCommand> filteredModifications) throws Exception {
      if (command.isOnePhaseCommit()) {
         GlobalTransaction globalTransaction = command.getGlobalTransaction();
         filteredModifications.forEach(writeCommand -> addKeys(writeCommand.getAffectedKeys(), globalTransaction));
      }
   }

   void backupWrite(DataWriteCommand command) {
      addKeys(command.getAffectedKeys(), command.getCommandInvocationId());
   }

   void backupCommit(CommitCommand command, Collection<WriteCommand> filteredModifications) throws Exception {
      GlobalTransaction globalTransaction = command.getGlobalTransaction();
      filteredModifications.forEach(writeCommand -> addKeys(writeCommand.getAffectedKeys(), globalTransaction));
   }

   void backupRollback(RollbackCommand command) throws Exception {
      //no-op
   }

   private void addKeys(Collection<?> keys, Object lastWriter) {
      keys.forEach(key -> modifiedKeys.put(key, lastWriter));
   }

   //invoked by single thread
   private void backupRemotelyIteration() {
      if (!siteStatusManager.hasAsyncSites()) {
         modifiedKeys.clear();
         return; //no async sites
      }
      try {
         if (isResponsePending()) {
            return;
         }
         backRemotely();
      } catch (Exception e) {
         e.printStackTrace();  // TODO: Customise this generated block
      }

   }

   private AsyncUpdateCommand newCommand() {
      return commandsFactory.buildAsyncUpdateCommand(batchSize);
   }

   private void backRemotely() throws Exception {
      final LocalizedCacheTopology cacheTopology = clusteringDependentLogic.getCacheTopology();
      final Iterator<Map.Entry<Object, Object>> iterator = modifiedKeys.entrySet().iterator();
      AsyncUpdateCommand command = newCommand();
      Map<Object, Object> currentBatch = new HashMap<>();
      LastIteration lastIteration = new LastIteration();

      while (iterator.hasNext()) {
         Map.Entry<Object, Object> entry = iterator.next();
         Object key = entry.getKey();
         if (canIgnore(cacheTopology, iterator, key)) {
            continue;
         }
         //we are the primary owner
         addInternalCacheEntry(fetchEntry(key), command, currentBatch, key, entry.getValue());

         if (currentBatch.size() == batchSize) {
            sendBatch(lastIteration, command, currentBatch);
            currentBatch = new HashMap<>();
            command = newCommand();
         }
      }
      if (currentBatch.size() != 0) {
         sendBatch(lastIteration, command, currentBatch);
      }
      this.lastIteration = lastIteration;
   }


   private void sendBatch(LastIteration lastIteration, AsyncUpdateCommand command, Map<Object, Object> batch)
         throws Exception {
      lastIteration.responseList.add(transport.backupRemotely(siteStatusManager.calculateAsyncBackups(), command));
      lastIteration.batchList.add(batch);
   }

   private void addInternalCacheEntry(InternalCacheEntry<Object, Object> entry, AsyncUpdateCommand command,
         Map<Object, Object> batch, Object key, Object lastWriter) {
      if (entry == null) {
         command.addUpdate(AsyncUpdate.removal(key));
      } else {
         command.addUpdate(AsyncUpdate.update(key, entry.getValue(), entry.getMetadata()));
      }
      batch.put(key, lastWriter);
   }

   private boolean canIgnore(LocalizedCacheTopology cacheTopology, Iterator<?> iterator, Object key) {
      switch (cacheTopology.getDistribution(key).writeOwnership()) {
         case NON_OWNER:
            iterator.remove();
         case BACKUP:
            return true;
         default:
            return false;
      }
   }

   private InternalCacheEntry<Object, Object> fetchEntry(Object key) {
      return loadAndStoreInDataContainer(dataContainer, persistenceManager, key, new AsyncXSiteInvocationContext(),
            timeService, null);
   }

   private boolean isResponsePending() throws Exception {
      LastIteration iteration = lastIteration;
      if (iteration == null) {
         return false;
      }
      //TODO check outcome
      while (iteration.hasNext()) {
         BackupResponse response = iteration.next();
         if (response.isDone()) {
            Map<Object, Object> batch = iteration.remove();
            response.waitForBackupToFinish();
            siteStatusManager.updateOfflineSites(response);
            if (response.getFailedBackups().isEmpty()) {
               //all sites updated correctly.
               for (Map.Entry<Object, Object> entry : batch.entrySet()) {
                  modifiedKeys.remove(entry.getKey(), entry.getValue());
               }
            }
         } else {
            return true;
         }
      }
      if (iteration.hasNext()) {
         lastIteration = null;
      }
      return false;
   }

   private static class LastIteration {

      private final List<BackupResponse> responseList;
      private final List<Map<Object, Object>> batchList;

      private LastIteration() {
         batchList = new LinkedList<>();
         responseList = new LinkedList<>();
      }

      boolean hasNext() {
         return !responseList.isEmpty();
      }

      BackupResponse next() {
         return responseList.get(0);
      }

      Map<Object, Object> remove() {
         responseList.remove(0);
         return batchList.remove(0);
      }
   }

   private static class AsyncXSiteInvocationContext extends AbstractInvocationContext {

      private AsyncXSiteInvocationContext() {
         super(null);
      }

      @Override
      public CacheEntry lookupEntry(Object key) {
         return null;
      }

      @Override
      public Map<Object, CacheEntry> getLookedUpEntries() {
         return Collections.emptyMap();
      }

      @Override
      public void putLookedUpEntry(Object key, CacheEntry e) {

      }

      @Override
      public void removeLookedUpEntry(Object key) {

      }

      @Override
      public boolean isInTxScope() {
         return false;
      }

      @Override
      public Object getLockOwner() {
         return null;
      }

      @Override
      public void setLockOwner(Object lockOwner) {
      }

      @Override
      public Set<Object> getLockedKeys() {
         return Collections.emptySet();
      }

      @Override
      public void clearLockedKeys() {

      }

      @Override
      public void addLockedKey(Object key) {
      }

      @Override
      public boolean isOriginLocal() {
         return true;
      }
   }
}
