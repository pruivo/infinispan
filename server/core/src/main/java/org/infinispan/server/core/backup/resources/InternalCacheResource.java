package org.infinispan.server.core.backup.resources;

import static org.infinispan.commons.internal.InternalCacheNames.PROTOBUF_METADATA_CACHE_NAME;
import static org.infinispan.commons.internal.InternalCacheNames.SCRIPT_CACHE_NAME;
import static org.infinispan.server.core.BackupManager.Resources.Type.PROTO_SCHEMAS;
import static org.infinispan.server.core.BackupManager.Resources.Type.TASKS;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.zip.ZipFile;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.CacheException;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.server.core.BackupManager;
import org.infinispan.server.core.backup.ContainerResource;
import org.infinispan.util.concurrent.BlockingManager;

/**
 * {@link org.infinispan.server.core.backup.ContainerResource} implementation for {@link
 * BackupManager.Resources.Type#PROTO_SCHEMAS} and {@link BackupManager.Resources.Type#TASKS}.
 *
 * @author Ryan Emerson
 * @since 12.0
 */
class InternalCacheResource extends AbstractContainerResource {

   private static final Map<BackupManager.Resources.Type, String> cacheMap = new HashMap<>(2);

   static {
      cacheMap.put(PROTO_SCHEMAS, PROTOBUF_METADATA_CACHE_NAME);
      cacheMap.put(TASKS, SCRIPT_CACHE_NAME);
   }

   private final AdvancedCache<String, String> cache;

   private InternalCacheResource(BackupManager.Resources.Type type, AdvancedCache<String, String> cache,
                                 BlockingManager blockingManager, BackupManager.Resources params, Path root) {
      super(type, params, blockingManager, root);
      this.cache = cache;
   }

   static ContainerResource create(BackupManager.Resources.Type type, BlockingManager blockingManager, EmbeddedCacheManager cm,
                                   BackupManager.Resources params, Path root) {
      String cacheName = cacheMap.get(type);
      if (SecurityActions.getCacheConfiguration(cm, cacheName) == null)
         return null;

      AdvancedCache<String, String> cache = SecurityActions.getUnwrappedCache(cm.getCache(cacheName));
      return new InternalCacheResource(type, cache, blockingManager, params, root);
   }

   @Override
   public void prepareAndValidateBackup() {
      if (wildcard) {
         resources.addAll(cache.keySet());
         return;
      }

      for (String fileName : resources) {
         if (!cache.containsKey(fileName))
            throw log.unableToFindResource(type.toString(), fileName);
      }
   }

   @Override
   public CompletionStage<Void> backup() {
      return blockingManager.runBlocking(() -> {
         mkdirs(root);
         for (Map.Entry<String, String> entry : cache.entrySet()) {
            String fileName = entry.getKey();
            if (resources.contains(fileName)) {
               Path file = root.resolve(fileName);
               try {
                  // ISPN-13591 Create directory structure if resource name contains path separator
                  Path parent = file.getParent();
                  if (!parent.equals(root)) {
                     Files.createDirectories(parent);
                  }
                  Files.write(file, entry.getValue().getBytes(StandardCharsets.UTF_8));
               } catch (IOException e) {
                  throw new CacheException(String.format("Unable to create %s", file), e);
               }
               log.debugf("Backup up %s %s", type, fileName);
            }
         }
      }, "write-" + type.toString());
   }

   @Override
   public CompletionStage<Void> restore(ZipFile zip) {
      return blockingManager.runBlocking(() -> {
         for (String file : resources) {
            String zipPath = root.resolve(file).toString();
            try (InputStream is = zip.getInputStream(zip.getEntry(zipPath));
                 BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
               String content = reader.lines().collect(Collectors.joining("\n"));
               cache.put(file, content);
               log.debugf("Restoring %s %s", type, file);
            } catch (IOException e) {
               throw new CacheException(e);
            }
         }
      }, "restore-" + type.toString());
   }
}
