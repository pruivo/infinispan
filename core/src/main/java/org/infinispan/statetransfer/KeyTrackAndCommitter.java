package org.infinispan.statetransfer;

import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.commons.util.concurrent.jdk8backported.EquivalentConcurrentHashMapV8;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.metadata.Metadata;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.EnumSet;

/**
 * Keeps track of the keys updated by normal operation and state transfer. Since the command processing happens
 * concurrently with the state transfer, it needs to keep track of the keys updated by normal command in order to reject
 * the updates from the state transfer. It assumes that the keys from normal operations are most recent thant the ones
 * received by state transfer.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class KeyTrackAndCommitter {

   private static final Log log = LogFactory.getLog(KeyTrackAndCommitter.class);
   private static final boolean trace = log.isTraceEnabled();
   private static final TrackEquivalence TRACK_EQUIVALENCE = new TrackEquivalence();
   private final EquivalentConcurrentHashMapV8<Object, EnumSet<Flag>> tracker;
   private DataContainer dataContainer;
   private volatile boolean trackStateTransfer;
   private volatile boolean trackXSiteStateTransfer;

   public KeyTrackAndCommitter(Equivalence<Object> keyEq) {
      tracker = new EquivalentConcurrentHashMapV8<Object, EnumSet<Flag>>(keyEq, TRACK_EQUIVALENCE);
   }

   @Inject
   public final void inject(DataContainer dataContainer) {
      this.dataContainer = dataContainer;
   }

   /**
    * It starts tracking keys committed. All the keys committed will be flagged with this flag. State transfer received
    * after the key is tracked will be discarded.
    *
    * @param track Flag to start tracking keys for local site state transfer or for remote site state transfer.
    */
   public final void startTrack(Flag track) {
      setTrack(track, true);
   }

   /**
    * It stops tracking keys committed.
    *
    * @param track Flag to stop tracking keys for local site state transfer or for remote site state transfer.
    */
   public final void stopTrack(Flag track) {
      setTrack(track, false);
      if (!trackStateTransfer && !trackXSiteStateTransfer) {
         if (trace) {
            log.tracef("Tracking is disabled. Clear tracker: %s", tracker);
         }
         tracker.clear();
      }
   }

   /**
    * It tries to commit the cache entry. The entry is not committed if it is originated from state transfer and other
    * operation already has updated it.
    *
    * @param entry    the entry to commit
    * @param metadata the entry's metadata
    * @param track    if {@code null}, it identifies this commit as originated from a normal operation. Otherwise, it is
    *                 originated from a state transfer (local or remote site)
    */
   public final void commit(final CacheEntry entry, final Metadata metadata, final Flag track, boolean l1Invalidation) {
      if (trace) {
         log.tracef("Trying to commit. Key=%s. Track Flag=%s, L1 invalidation=%s", entry.getKey(), track, l1Invalidation);
      }
      if (l1Invalidation || (track == null && !trackStateTransfer && !trackXSiteStateTransfer)) {
         //track == null means that it is a normal put and the tracking is not enabled!
         //if it is a L1 invalidation, commit without track it.
         if (trace) {
            log.tracef("Committing key=%s. It is a L1 invalidation or a normal put and no track is enabled!", entry.getKey());
         }
         entry.commit(dataContainer, metadata);
         return;
      }
      if ((track == Flag.PUT_FOR_STATE_TRANSFER && !trackStateTransfer) ||
            (track == Flag.PUT_FOR_X_SITE_STATE_TRANSFER && !trackXSiteStateTransfer)) {
         //this a put for state transfer but we are not tracking it. This means that the state transfer has ended
         //or canceled due to a clear command.
         if (trace) {
            log.tracef("Not committing key=%s. It is a state transfer key but no track is enabled!", entry.getKey());
         }
         return;
      }
      tracker.compute(entry.getKey(), new EquivalentConcurrentHashMapV8.BiFun<Object, EnumSet<Flag>, EnumSet<Flag>>() {
         @Override
         public EnumSet<Flag> apply(Object o, EnumSet<Flag> tracks) {
            if (tracks != null && tracks.contains(track)) {
               if (trace) {
                  log.tracef("Not committing key=%s. It was already overwritten! Track Set=%s", entry.getKey(), tracks);
               }
               return tracks;
            }
            entry.commit(dataContainer, metadata);
            EnumSet<Flag> newTracks = calculateTrackSet(tracks);
            if (trace) {
               log.tracef("Committed key=%s. Old track set=%s. New track set=%s", entry.getKey(), tracks, newTracks);
            }
            return newTracks;
         }
      });
   }

   /**
    * @return {@code true} if the flag is being tracked, {@code false} otherwise.
    */
   public final boolean isTracking(Flag trackFlag) {
      switch (trackFlag) {
         case PUT_FOR_STATE_TRANSFER:
            return trackStateTransfer;
         case PUT_FOR_X_SITE_STATE_TRANSFER:
            return trackXSiteStateTransfer;
      }
      return false;
   }

   /**
    * @return {@code true} if no keys are tracked, {@code false} otherwise.
    */
   public final boolean isEmpty() {
      return tracker.isEmpty();
   }

   private void setTrack(Flag track, boolean value) {
      if (trace) {
         log.tracef("Set track to %s = %s", track, value);
      }
      switch (track) {
         case PUT_FOR_STATE_TRANSFER:
            this.trackStateTransfer = value;
            return;
         case PUT_FOR_X_SITE_STATE_TRANSFER:
            this.trackXSiteStateTransfer = value;
      }
   }

   private EnumSet<Flag> calculateTrackSet(EnumSet<Flag> existing) {
      EnumSet<Flag> newValue = existing == null ? EnumSet.noneOf(Flag.class) : existing;
      if (trackStateTransfer && trackXSiteStateTransfer) {
         newValue.add(Flag.PUT_FOR_STATE_TRANSFER);
         newValue.add(Flag.PUT_FOR_X_SITE_STATE_TRANSFER);
      } else if (trackStateTransfer) {
         newValue.add(Flag.PUT_FOR_STATE_TRANSFER);
      } else if (trackXSiteStateTransfer) {
         newValue.add(Flag.PUT_FOR_X_SITE_STATE_TRANSFER);
      } else {
         newValue = null;
      }
      return newValue;
   }

   private static class TrackEquivalence implements Equivalence<EnumSet<Flag>> {

      private TrackEquivalence() {
      }

      @Override
      public int hashCode(Object obj) {
         return obj.hashCode();
      }

      @Override
      public boolean equals(EnumSet<Flag> obj, Object otherObj) {
         if (obj == null) return otherObj == null;
         return obj.equals(otherObj);
      }

      @Override
      public String toString(Object obj) {
         return String.valueOf(obj);
      }

      @Override
      public boolean isComparable(Object obj) {
         return false;
      }

      @Override
      public int compare(EnumSet<Flag> obj, EnumSet<Flag> otherObj) {
         return 0;
      }
   }

}
