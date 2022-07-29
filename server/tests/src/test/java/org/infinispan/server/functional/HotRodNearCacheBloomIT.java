package org.infinispan.server.functional;

import static org.infinispan.query.remote.client.ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.configuration.NearCacheMode;
import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerRuleBuilder;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

/**
 * TODO! document this
 */
public class HotRodNearCacheBloomIT {

   private static final String CACHE_CONFIG =
         "<distributed-cache name=\"CACHE_NAME\">\n"
               + "    <encoding media-type=\"application/x-protostream\"/>\n"
               + "</distributed-cache>";

   @ClassRule
   public static InfinispanServerRule SERVERS = InfinispanServerRuleBuilder.config("configuration/BasicServerTest.xml")
         .runMode(ServerRunMode.CONTAINER)
         .numServers(1)
         .build();

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

   @Test
   public void testWithBloomFilter() {
      doNearCacheTest("bloom-enabled", "bloom-key", true);
   }

   @Test
   public void testWithoutBloomFilter() {
      doNearCacheTest("bloom-disabled", "key", false);
   }

   private void doNearCacheTest(String cacheName, String key, boolean bloomFilter) {
      try (RemoteCacheManager client = getHotRodClient(cacheName, bloomFilter)) {
         CustomObject k = new CustomObject(key);
         RemoteCache<CustomObject, CustomObject> cache = client.getCache(cacheName);
         cache.put(k, new CustomObject("value"));
         cache.remove(k);
      }
   }

   private RemoteCacheManager getHotRodClient(String cacheName, boolean bloomFilter) {
      GeneratedSchema schema = new CustomObjectSchemaImpl();
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.remoteCache(cacheName).configuration(CACHE_CONFIG.replace("CACHE_NAME", cacheName))
            .nearCacheMode(NearCacheMode.INVALIDATED)
            .nearCacheUseBloomFilter(bloomFilter)
            .nearCacheMaxEntries(1000);

      builder.addContextInitializer(schema);

      RemoteCacheManager manager = SERVER_TEST.hotrod()
            .withClientConfiguration(builder)
            .createRemoteCacheManager();

      RemoteCache<String, String> metadataCache = manager.getCache(PROTOBUF_METADATA_CACHE_NAME);
      metadataCache.putIfAbsent(schema.getProtoFileName(), schema.getProtoFile());
      return manager;
   }

   public static class CustomObject {
      @ProtoField(1)
      public String value;

      @ProtoFactory
      public CustomObject(String value) {
         this.value = value;
      }

      @Override
      public String toString() {
         return "CustomObject{" +
               "value='" + value + '\'' +
               '}';
      }
   }

   @AutoProtoSchemaBuilder(schemaPackageName = "hotrod.near.cache", includeClasses = CustomObject.class)
   public interface CustomObjectSchema extends GeneratedSchema {

   }

}
