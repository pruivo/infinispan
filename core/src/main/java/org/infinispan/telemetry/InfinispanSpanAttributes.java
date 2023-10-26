package org.infinispan.telemetry;

import java.util.Objects;
import java.util.Optional;

import org.infinispan.configuration.cache.Configuration;

public class InfinispanSpanAttributes {

   private final String cacheName;
   private final Configuration cacheConfiguration;
   private final SpanCategory category;

   private InfinispanSpanAttributes(String cacheName, Configuration cacheConfiguration, SpanCategory category) {
      this.cacheName = cacheName;
      this.cacheConfiguration = cacheConfiguration;
      this.category = category;
   }

   public Optional<String> getCacheName() {
      return Optional.ofNullable(cacheName);
   }

   public SpanCategory getCategory() {
      return category;
   }

   public boolean isCategoryDisabled() {
      // TODO impl
      return false;
   }

   public static class Builder {
      private String cacheName;
      private Configuration cacheConfiguration;
      private SpanCategory category;

      public Builder(SpanCategory category) {
         // category is mandatory
         withCategory(category);
      }

      public Builder withCache(String cacheName, Configuration cacheConfiguration) {
         this.cacheName = cacheName;
         this.cacheConfiguration = cacheConfiguration;
         return this;
      }

      public Builder withCategory(SpanCategory category) {
         this.category = Objects.requireNonNull(category);
         return this;
      }

      public InfinispanSpanAttributes build() {
         return new InfinispanSpanAttributes(cacheName, cacheConfiguration, category);
      }
   }

}
