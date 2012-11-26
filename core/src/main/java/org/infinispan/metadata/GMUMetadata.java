/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.metadata;

import org.infinispan.container.versioning.EntryVersion;

import java.util.concurrent.TimeUnit;

/**
 * Metadata used by the GMU algorithm.
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
public class GMUMetadata implements Metadata {

   private final Metadata metadata;
   private final EntryVersion creationVersion;
   private final EntryVersion maxValidVersion;
   private final EntryVersion transactionVersion;
   private final boolean mostRecent;

   private GMUMetadata(Metadata metadata, EntryVersion creationVersion, EntryVersion maxValidVersion,
                       EntryVersion transactionVersion, boolean mostRecent) {
      this.metadata = metadata;
      this.creationVersion = creationVersion;
      this.maxValidVersion = maxValidVersion;
      this.transactionVersion = transactionVersion;
      this.mostRecent = mostRecent;
   }

   @Override
   public final long lifespan() {
      return metadata.lifespan();
   }

   @Override
   public final long maxIdle() {
      return metadata.maxIdle();
   }

   @Override
   public final EntryVersion version() {
      return metadata.version();
   }

   @Override
   public final Builder builder() {
      Builder other = metadata.builder();
      return new GMUBuilder(other).creationVersion(creationVersion).maximumValidVersion(maxValidVersion)
            .transactionVersion(transactionVersion).mostRecent(mostRecent);
   }

   public final Metadata getOriginalMetadata() {
      return metadata;
   }

   public static GMUBuilder fromMetadata(Metadata metadata) {
      if (metadata == null) {
         return new GMUBuilder();
      }
      return metadata instanceof GMUMetadata ? (GMUBuilder) metadata.builder() : new GMUBuilder(metadata.builder());
   }

   /**
    * @return complete version (from commit log) of when the version was created
    */
   public final EntryVersion creationVersion() {
      return creationVersion;
   }

   /**
    * @return the maximum version (from commit log) in each this value is valid
    */
   public final EntryVersion maximumValidVersion() {
      return maxValidVersion;
   }

   /**
    * @return the maximum version (from commit log) to update the transaction version
    */
   public final EntryVersion transactionVersion() {
      return transactionVersion;
   }

   /**
    * @return {@code true} if this entry is the most recent
    */
   public final boolean mostRecent() {
      return mostRecent;
   }

   public static class GMUBuilder implements Builder {

      private final Builder builder;
      private EntryVersion creationVersion;
      private EntryVersion maxValidVersion;
      private EntryVersion transactionVersion;
      private boolean mostRecent;

      public GMUBuilder() {
         this.builder = new EmbeddedMetadata.Builder();
      }

      public GMUBuilder(Builder builder) {
         this.builder = builder;
      }

      @Override
      public final Builder lifespan(long time, TimeUnit unit) {
         builder.lifespan(time, unit);
         return this;
      }

      @Override
      public final Builder lifespan(long time) {
         builder.lifespan(time);
         return this;
      }

      @Override
      public final Builder maxIdle(long time, TimeUnit unit) {
         builder.maxIdle(time, unit);
         return this;
      }

      @Override
      public final Builder maxIdle(long time) {
         builder.maxIdle(time);
         return this;
      }

      @Override
      public final Builder version(EntryVersion version) {
         builder.version(version);
         return this;
      }

      @Override
      public final Metadata build() {
         return new GMUMetadata(builder.build(), creationVersion, maxValidVersion, transactionVersion, mostRecent);
      }

      /**
       * Sets the creation version. See {@link org.infinispan.metadata.GMUMetadata#creationVersion()}
       *
       * @param creationVersion
       * @return a builder instance with the creation version applied
       */
      public final GMUBuilder creationVersion(EntryVersion creationVersion) {
         this.creationVersion = creationVersion;
         return this;
      }

      /**
       * Sets the maximum valid version. See {@link org.infinispan.metadata.GMUMetadata#maximumValidVersion()}.
       *
       * @param maximumValidVersion
       * @return a builder instance with the maximum valid version applied
       */
      public final GMUBuilder maximumValidVersion(EntryVersion maximumValidVersion) {
         this.maxValidVersion = maximumValidVersion;
         return this;
      }

      /**
       * Sets the transaction version. See {@link org.infinispan.metadata.GMUMetadata#transactionVersion()}.
       *
       * @param transactionVersion
       * @return a builder instance with the transaction version applied
       */
      public final GMUBuilder transactionVersion(EntryVersion transactionVersion) {
         this.transactionVersion = transactionVersion;
         return this;
      }

      /**
       * Flags the entry as the most recent version available. See {@link org.infinispan.metadata.GMUMetadata#mostRecent()}.
       *
       * @param mostRecent
       * @return a builder instance with the most recent flag applied
       */
      public final GMUBuilder mostRecent(boolean mostRecent) {
         this.mostRecent = mostRecent;
         return this;
      }

   }

}
