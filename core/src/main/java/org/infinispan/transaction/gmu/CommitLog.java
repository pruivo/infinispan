package org.infinispan.transaction.gmu;


import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.InequalVersionComparisonResult;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.container.versioning.gmu.GMUReadVersion;
import org.infinispan.container.versioning.gmu.GMUVersion;
import org.infinispan.container.versioning.gmu.GMUVersionGenerator;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.gmu.manager.CommittedTransaction;
import org.infinispan.util.Util;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Set;

import static org.infinispan.container.versioning.InequalVersionComparisonResult.*;
import static org.infinispan.container.versioning.gmu.GMUVersion.NON_EXISTING;
import static org.infinispan.transaction.gmu.GMUHelper.toGMUVersion;
import static org.infinispan.transaction.gmu.GMUHelper.toGMUVersionGenerator;

/**
 * @author Pedro Ruivo
 * @author Sebastiano Peluso
 * @since 5.2
 */
public class CommitLog {

   private static final Log log = LogFactory.getLog(CommitLog.class);
   private GMUVersion mostRecentVersion;
   private VersionEntry currentVersion;
   private GMUVersionGenerator versionGenerator;
   private boolean enabled = false;

   @Inject
   public void inject(VersionGenerator versionGenerator, Configuration configuration) {
      if (configuration.locking().isolationLevel() == IsolationLevel.SERIALIZABLE) {
         this.versionGenerator = toGMUVersionGenerator(versionGenerator);
      }
      enabled = this.versionGenerator != null;
   }

   //AFTER THE VersionVCFactory
   @Start(priority = 31)
   public void start() {
      if (!enabled) {
         return;
      }
      currentVersion = new VersionEntry(toGMUVersion(versionGenerator.generateNew()), Collections.emptySet(), 0);
      mostRecentVersion = toGMUVersion(versionGenerator.generateNew());
   }

   @Stop
   public void stop() {

   }

   public final void initLocalTransaction(LocalTransaction localTransaction) {
      if (!enabled) {
         return;
      }
      GMUVersion transactionVersion;
      synchronized (this) {
         transactionVersion = versionGenerator.updatedVersion(mostRecentVersion);
      }
      localTransaction.setTransactionVersion(transactionVersion);
   }

   public final synchronized GMUVersion getCurrentVersion() {
      assertEnabled();
      //versions are immutable
      GMUVersion version = versionGenerator.updatedVersion(mostRecentVersion);
      //GMUVersion version = versionGenerator.updatedVersion(currentVersion.getVersion());
      if (log.isTraceEnabled()) {
         log.tracef("getCurrentVersion() ==> %s", version);
      }
      return version;
   }

   public final EntryVersion getOldestVersion() {
      VersionEntry iterator;
      synchronized (this) {
         iterator = currentVersion;
      }
      while (iterator.getPrevious() != null) {
         iterator = iterator.getPrevious();
      }
      return iterator.getVersion();
   }

   public final EntryVersion getEntry(EntryVersion entryVersion) {
      GMUVersion gmuEntryVersion = toGMUVersion(entryVersion);
      VersionEntry versionEntry;
      synchronized (this) {
         versionEntry = currentVersion;
      }
      while (versionEntry != null) {
         if (versionEntry.getVersion().getThisNodeVersionValue() == gmuEntryVersion.getThisNodeVersionValue()) {
            return versionEntry.getVersion();
         }
         versionEntry = versionEntry.getPrevious();
      }
      return getOldestVersion();
   }

   public final GMUVersion getAvailableVersionLessThan(EntryVersion other) {
      assertEnabled();
      if (other == null) {
         synchronized (this) {
            return versionGenerator.updatedVersion(mostRecentVersion);
            //return versionGenerator.updatedVersion(currentVersion.getVersion());
         }
      }
      GMUVersion gmuVersion = toGMUVersion(other);

      if (gmuVersion.getThisNodeVersionValue() != NON_EXISTING) {
         return gmuVersion;
      }

      LinkedList<GMUVersion> possibleVersion = new LinkedList<GMUVersion>();
      VersionEntry iterator;
      synchronized (this) {
         iterator = currentVersion;
      }

      while (iterator != null) {
         if (isLessOrEquals(iterator.getVersion(), gmuVersion)) {
            possibleVersion.add(iterator.getVersion());
         }
         iterator = iterator.getPrevious();
      }
      return versionGenerator.mergeAndMax(possibleVersion.toArray(new GMUVersion[possibleVersion.size()]));
   }

   public final GMUReadVersion getReadVersion(EntryVersion other) {
      if (other == null) {
         return null;
      }
      GMUVersion gmuVersion = toGMUVersion(other);
      GMUReadVersion gmuReadVersion = versionGenerator.convertVersionToRead(gmuVersion);
      VersionEntry iterator;
      synchronized (this) {
         iterator = currentVersion;
      }

      while (iterator != null) {
         if (log.isTraceEnabled()) {
            log.tracef("getReadVersion(...) ==> comparing %s and %s", iterator.getVersion(), gmuReadVersion);
         }
         if (iterator.getVersion().getThisNodeVersionValue() <= gmuReadVersion.getThisNodeVersionValue()) {
            if (!isLessOrEquals(iterator.getVersion(), gmuVersion)) {
               if (log.isTraceEnabled()) {
                  log.tracef("getReadVersion(...) ==> comparing %s and %s ==> NOT VISIBLE", iterator.getVersion(), gmuReadVersion);
               }
               gmuReadVersion.addNotVisibleSubversion(iterator.getVersion().getThisNodeVersionValue(), iterator.getSubVersion());
            } else {
               if (log.isTraceEnabled()) {
                  log.tracef("getReadVersion(...) ==> comparing %s and %s ==> VISIBLE", iterator.getVersion(), gmuReadVersion);
               }
            }
         } else {
            if (log.isTraceEnabled()) {
               log.tracef("getReadVersion(...) ==> comparing %s and %s ==> IGNORE", iterator.getVersion(), gmuReadVersion);
            }
         }
         iterator = iterator.getPrevious();
         if (log.isTraceEnabled()) {
            log.tracef("getReadVersion(...) ==> next version: %s", iterator);
         }
      }
      return gmuReadVersion;
   }

   public final synchronized void insertNewCommittedVersions(Collection<CommittedTransaction> transactions) {
      assertEnabled();
      for (CommittedTransaction transaction : transactions) {
         if (log.isTraceEnabled()) {
            log.tracef("insertNewCommittedVersions(...) ==> add %s", transaction.getCommitVersion());
         }
         VersionEntry current = new VersionEntry(toGMUVersion(transaction.getCommitVersion()),
                                                 Util.getAffectedKeys(transaction.getModifications(), null),
                                                 transaction.getSubVersion());
         current.setPrevious(currentVersion);
         currentVersion = current;
         mostRecentVersion = versionGenerator.mergeAndMax(mostRecentVersion, currentVersion.getVersion());
      }
      if (log.isTraceEnabled()) {
         log.tracef("insertNewCommittedVersions(...) ==> %s", currentVersion.getVersion());
      }
      notifyAll();
   }

   public final synchronized void updateMostRecentVersion(EntryVersion newVersion) {
      /*
      assertEnabled();
      GMUVersion gmuEntryVersion = toGMUVersion(newVersion);
      if (gmuEntryVersion.getThisNodeVersionValue() > mostRecentVersion.getThisNodeVersionValue()) {
         log.warn("Cannot update the most recent version to a version higher than " +
                                                  "the current version");
         return;
      }
      mostRecentVersion = versionGenerator.mergeAndMax(mostRecentVersion, gmuEntryVersion);
      */
   }

   public final synchronized boolean waitForVersion(EntryVersion version, long timeout) throws InterruptedException {
      assertEnabled();
      if (timeout < 0) {
         if (log.isTraceEnabled()) {
            log.tracef("waitForVersion(%s,%s) and current version is %s", version, timeout, currentVersion.getVersion());
         }
         long versionValue = toGMUVersion(version).getThisNodeVersionValue();
         while (currentVersion.getVersion().getThisNodeVersionValue() < versionValue) {
            wait();
         }
         if (log.isTraceEnabled()) {
            log.tracef("waitForVersion(%s) ==> %s TRUE ?", version,
                       currentVersion.getVersion().getThisNodeVersionValue());
         }
         return true;
      }
      long finalTimeout = System.currentTimeMillis() + timeout;
      long versionValue = toGMUVersion(version).getThisNodeVersionValue();
      if (log.isTraceEnabled()) {
         log.tracef("waitForVersion(%s,%s) and current version is %s", version, timeout, currentVersion.getVersion());
      }
      do {
         if (currentVersion.getVersion().getThisNodeVersionValue() >= versionValue) {
            if (log.isTraceEnabled()) {
               log.tracef("waitForVersion(%s) ==> %s >= %s ?", version,
                          currentVersion.getVersion().getThisNodeVersionValue(), versionValue);
            }
            return true;
         }
         long waitingTime = finalTimeout - System.currentTimeMillis();
         if (waitingTime <= 0) {
            break;
         }
         wait(waitingTime);
      } while (true);
      if (log.isTraceEnabled()) {
         log.tracef("waitForVersion(%s) ==> %s >= %s ?", version,
                    currentVersion.getVersion().getThisNodeVersionValue(), versionValue);
      }
      return currentVersion.getVersion().getThisNodeVersionValue() >= versionValue;
   }

   public final boolean dumpTo(String filePath) {
      assertEnabled();
      BufferedWriter bufferedWriter = Util.getBufferedWriter(filePath);
      if (bufferedWriter == null) {
         return false;
      }
      try {
         VersionEntry iterator;
         synchronized (this) {
            //bufferedWriter.write(mostRecentVersion.toString());
            iterator = currentVersion;
         }
         bufferedWriter.newLine();
         while (iterator != null) {
            iterator.dumpTo(bufferedWriter);
            iterator = iterator.getPrevious();
         }
         return true;
      } catch (IOException e) {
         return false;
      } finally {
         Util.close(bufferedWriter);
      }
   }

   /**
    * removes the older version than {@param minVersion} and returns the minimum usable version to remove the old values
    * in data container
    *
    * @param minVersion the minimum visible version
    * @return the minimum usable version (to remove entries in data container)
    */
   public final GMUVersion gcOlderVersions(GMUVersion minVersion) {
      VersionEntry iterator;
      VersionEntry removeFromHere = null;
      GMUVersion minimumVisibleVersion = null;
      synchronized (this) {
         iterator = currentVersion;
      }

      while (iterator != null) {
         if (isLessOrEquals(iterator.getVersion(), minVersion)) {
            if (minimumVisibleVersion == null) {
               minimumVisibleVersion = iterator.getVersion();
               removeFromHere = iterator;
            }
         } else {
            minimumVisibleVersion = null;
            removeFromHere = null;
         }
         iterator = iterator.getPrevious();
      }

      while (removeFromHere != null) {
         VersionEntry previous = removeFromHere.getPrevious();
         removeFromHere.setPrevious(null);
         removeFromHere = previous;
      }

      if (log.isTraceEnabled()) {
         log.tracef("gcOlderVersion(%s) ==> %s", minVersion, minimumVisibleVersion);
      }

      return minimumVisibleVersion;
   }

   public final int calculateMinimumViewId() {
      VersionEntry iterator;
      int minimumViewId;
      synchronized (this) {
         minimumViewId = currentVersion.getVersion().getViewId();
         iterator = currentVersion.getPrevious();
      }
      while (iterator != null) {
         minimumViewId = Math.min(minimumViewId, iterator.getVersion().getViewId());
      }
      return minimumViewId;
   }

   private void assertEnabled() {
      if (!enabled) {
         throw new IllegalStateException("Commit Log not enabled!");
      }
   }

   private boolean isLessOrEquals(EntryVersion version1, EntryVersion version2) {
      InequalVersionComparisonResult comparisonResult = version1.compareTo(version2);
      return comparisonResult == BEFORE_OR_EQUAL || comparisonResult == BEFORE || comparisonResult == EQUAL;
   }

   private static class VersionEntry {
      private final GMUVersion version;
      private final Object[] keysModified;
      private final int subVersion;
      private VersionEntry previous;

      private VersionEntry(GMUVersion version, Set<Object> keysModified, int subVersion) {
         this.version = version;
         if (keysModified == null) {
            this.keysModified = null;
         } else {
            this.keysModified = keysModified.toArray(new Object[keysModified.size()]);
         }
         this.subVersion = subVersion;
      }

      public GMUVersion getVersion() {
         return version;
      }

      public VersionEntry getPrevious() {
         return previous;
      }

      public void setPrevious(VersionEntry previous) {
         this.previous = previous;
      }

      public int getSubVersion() {
         return subVersion;
      }

      @Override
      public String toString() {
         return "VersionEntry{" +
               "version=" + version +
               ", subVersion=" + subVersion +
               ", keysModified=" + (keysModified == null ? "ALL" : Arrays.asList(keysModified)) +
               '}';
      }

      public final void dumpTo(BufferedWriter writer) throws IOException {
         writer.write(version.toString());
         writer.write("=");
         writer.write((keysModified == null ? "ALL" : Arrays.asList(keysModified).toString()));
         writer.newLine();
         writer.flush();
      }
   }
}
