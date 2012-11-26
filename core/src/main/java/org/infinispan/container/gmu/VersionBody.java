package org.infinispan.container.gmu;

import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.InequalVersionComparisonResult;

import static org.infinispan.container.versioning.InequalVersionComparisonResult.*;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public abstract class VersionBody<T> {
   private final T value;
   private VersionBody<T> previous;

   protected VersionBody(T value) {
      this.value = value;
   }

   protected static boolean isOlder(EntryVersion older, EntryVersion newer) {
      return older.compareTo(newer) == InequalVersionComparisonResult.BEFORE;
   }

   protected static boolean isOlderOrEquals(EntryVersion older, EntryVersion newer) {
      InequalVersionComparisonResult result = older.compareTo(newer);
      return result == BEFORE || result == BEFORE_OR_EQUAL || result == EQUAL;
   }

   protected static boolean isEqual(EntryVersion older, EntryVersion newer) {
      return older.compareTo(newer) == InequalVersionComparisonResult.EQUAL;
   }

   public T getValue() {
      return value;
   }

   public synchronized VersionBody<T> getPrevious() {
      return previous;
   }

   public synchronized void setPrevious(VersionBody<T> previous) {
      this.previous = previous;
   }

   public synchronized VersionBody<T> add(VersionBody<T> other) {
      if (previous == null) {
         previous = other;
         return null;
      } else if (previous.isOlder(other)) {
         other.setPrevious(previous);
         setPrevious(other);
         return null;
      } else if (previous.isEqual(other)) {
         previous.reincarnate(other);
         return null;
      }
      return previous;
   }

   public synchronized VersionBody expire(long now) {
      if (previous == null) {
         return null;
      }
      if (previous.isExpired(now)) {
         previous = previous.getPrevious();
         return this;
      }
      return previous;
   }

   @Override
   public String toString() {
      return "VersionBody{" +
            "value=" + value +
            '}';
   }

   public abstract EntryVersion getVersion();

   public abstract boolean isOlder(VersionBody<T> otherBody);

   public abstract boolean isEqual(VersionBody<T> otherBody);

   public abstract boolean isOlderOrEquals(EntryVersion entryVersion);

   public abstract boolean isRemove();

   public abstract void reincarnate(VersionBody<T> other);

   public abstract VersionBody<T> gc(EntryVersion minVersion);

   protected abstract boolean isExpired(long now);

}
