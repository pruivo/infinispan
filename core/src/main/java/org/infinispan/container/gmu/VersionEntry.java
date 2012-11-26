package org.infinispan.container.gmu;

import org.infinispan.container.versioning.EntryVersion;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class VersionEntry<T> {
   private final T entry;
   private final EntryVersion nextVersion;
   private final boolean found;

   public VersionEntry(T entry, EntryVersion nextVersion, boolean found) {
      this.entry = entry;
      this.nextVersion = nextVersion;
      this.found = found;
   }

   public final T getEntry() {
      return entry;
   }

   public final boolean isMostRecent() {
      return nextVersion == null;
   }

   public final EntryVersion getNextVersion() {
      return nextVersion;
   }

   public final boolean isFound() {
      return found;
   }

   @Override
   public String toString() {
      return "VersionEntry{" +
            "entry=" + entry +
            ", nextVersion=" + nextVersion +
            ", found=" + found +
            '}';
   }
}
