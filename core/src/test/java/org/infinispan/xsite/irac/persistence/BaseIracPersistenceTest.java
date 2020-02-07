package org.infinispan.xsite.irac.persistence;

import java.io.File;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.container.versioning.irac.IracEntryVersion;
import org.infinispan.container.versioning.irac.TopologyIracVersion;
import org.infinispan.functional.impl.MetaParamsInternalMetadata;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.metadata.impl.IracMetaParam;
import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.MarshallableEntryFactory;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import net.jcip.annotations.GuardedBy;

/**
 * //TODO document this!
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
@Test(groups = "functional")
public abstract class BaseIracPersistenceTest<V> extends SingleCacheManagerTest {

   private static final AtomicLong V_GENERATOR = new AtomicLong();
   private static final String SITE = "LON";
   protected String tmpDirectory;
   protected AdvancedLoadWriteStore<String, V> cacheStore;
   protected MarshallableEntryFactory<String, V> entryFactory;

   public void testWriteAndLoad(Method method) {
      String key = TestingUtil.k(method);
      String value = TestingUtil.v(method);
      IracMetadata metadata = createMetadata();

      cacheStore.write(createEntry(key, value, metadata));

      MarshallableEntry<String, V> loadedMEntry = cacheStore.loadEntry(key);


      assertCorrectEntry(loadedMEntry, key, value, metadata);
   }

   public void testWriteAndPublisher(Method method) {
      String key = TestingUtil.k(method);
      String value = TestingUtil.v(method);
      IracMetadata metadata = createMetadata();

      cacheStore.write(createEntry(key, value, metadata));

      MarshallableEntrySubscriber<V> subscriber = new MarshallableEntrySubscriber<>();
      cacheStore.entryPublisher(key::equals, true, true).subscribe(subscriber);


      List<MarshallableEntry<String, V>> entries = subscriber.cf.join();

      AssertJUnit.assertEquals(1, entries.size());
      assertCorrectEntry(entries.get(0), key, value, metadata);
   }

   @BeforeClass(alwaysRun = true)
   @Override
   protected void createBeforeClass() throws Exception {
      tmpDirectory = TestingUtil.tmpDirectory(this.getClass());
      Util.recursiveFileRemove(tmpDirectory);
      boolean created = new File(tmpDirectory).mkdirs();
      log.debugf("Created temporary directory %s (exists? %s)", tmpDirectory, !created);
      super.createBeforeClass();
   }

   @AfterClass(alwaysRun = true)
   @Override
   protected void destroyAfterClass() {
      super.destroyAfterClass();
      Util.recursiveFileRemove(tmpDirectory);
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfigurationBuilder gBuilder = createGlobalConfigurationBuilder();
      ConfigurationBuilder cBuilder = new ConfigurationBuilder();
      addPersistence(cBuilder);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(gBuilder, cBuilder);
      cacheStore = TestingUtil.getFirstLoader(cm.getCache());
      //noinspection unchecked
      entryFactory = TestingUtil.extractComponent(cm.getCache(), MarshallableEntryFactory.class);
      return cm;
   }

   protected SerializationContextInitializer getSerializationContextInitializer() {
      return TestDataSCI.INSTANCE;
   }

   protected abstract void addPersistence(ConfigurationBuilder builder);

   protected abstract V wrap(String key, String value);

   protected abstract String unwrap(V value);

   private GlobalConfigurationBuilder createGlobalConfigurationBuilder() {
      GlobalConfigurationBuilder builder = new GlobalConfigurationBuilder().nonClusteredDefault();
      builder.globalState().persistentLocation(tmpDirectory);
      builder.serialization().addContextInitializer(getSerializationContextInitializer());
      return builder;
   }

   private void assertCorrectEntry(MarshallableEntry<String, V> entry, String key, String value,
         IracMetadata metadata) {
      AssertJUnit.assertNotNull(entry);
      AssertJUnit.assertEquals(key, entry.getKey());
      AssertJUnit.assertEquals(value, unwrap(entry.getValue()));
      MetaParamsInternalMetadata internalMetadata = entry.getInternalMetadata();
      AssertJUnit.assertNotNull(internalMetadata);
      IracMetadata storedMetadata = internalMetadata.findMetaParam(IracMetaParam.class).map(IracMetaParam::get)
            .orElse(null);
      AssertJUnit.assertEquals(metadata, storedMetadata);
   }

   private MarshallableEntry<String, V> createEntry(String key, String value, IracMetadata metadata) {
      return entryFactory.create(key, wrap(key, value), null, wrapInternalMetadata(metadata), -1, -1);
   }

   private IracMetadata createMetadata() {
      TopologyIracVersion tVersion = new TopologyIracVersion(1, V_GENERATOR.incrementAndGet());
      IracEntryVersion version = new IracEntryVersion(Collections.singletonMap(SITE, tVersion));
      return new IracMetadata(SITE, version);
   }

   private MetaParamsInternalMetadata wrapInternalMetadata(IracMetadata metadata) {
      return new MetaParamsInternalMetadata.Builder()
            .add(new IracMetaParam(metadata))
            .build();
   }

   private static class MarshallableEntrySubscriber<V> implements Subscriber<MarshallableEntry<String, V>> {

      @GuardedBy("this")
      private final List<MarshallableEntry<String, V>> entries = new ArrayList<>(1);
      private final CompletableFuture<List<MarshallableEntry<String, V>>> cf = new CompletableFuture<>();

      @Override
      public void onSubscribe(Subscription subscription) {
         subscription.request(Long.MAX_VALUE);
      }

      @Override
      public synchronized void onNext(MarshallableEntry<String, V> entry) {
         entries.add(entry);
      }

      @Override
      public void onError(Throwable throwable) {
         cf.completeExceptionally(throwable);
      }

      @Override
      public synchronized void onComplete() {
         cf.complete(entries);
      }
   }

}
