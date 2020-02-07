package org.infinispan.xsite.irac;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.infinispan.Cache;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.util.IntSet;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.xa.GlobalTransaction;

import net.jcip.annotations.GuardedBy;

/**
 * //TODO document this!
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class ManualIracManager extends ControlledIracManager {

   @GuardedBy("this")
   private final Map<Object, Object> pendingKeys;
   @GuardedBy("this")
   private boolean enabled;

   private ManualIracManager(IracManager actual) {
      super(actual);
      pendingKeys = new HashMap<>();
   }

   public static ManualIracManager wrapCache(Cache<?, ?> cache) {
      IracManager iracManager = TestingUtil.extractComponent(cache, IracManager.class);
      if (iracManager instanceof ManualIracManager) {
         return (ManualIracManager) iracManager;
      }
      return TestingUtil.wrapComponent(cache, IracManager.class, ManualIracManager::new);
   }

   @Override
   public synchronized void trackUpdatedKey(Object key, Object lockOwner) {
      if (enabled) {
         pendingKeys.put(key, lockOwner);
      } else {
         super.trackUpdatedKey(key, lockOwner);
      }
   }

   @Override
   public synchronized <K> void trackUpdatedKeys(Collection<K> keys, Object lockOwner) {
      if (enabled) {
         keys.forEach(k -> pendingKeys.put(k, lockOwner));
      } else {
         super.trackUpdatedKeys(keys, lockOwner);
      }
   }

   @Override
   public synchronized void trackKeysFromTransaction(Stream<WriteCommand> modifications, GlobalTransaction lockOwner) {
      if (enabled) {
         DefaultIracManager defaultIracManager = asDefaultIracManager().get();
         defaultIracManager.keysFromMods(modifications).forEach(k -> pendingKeys.put(k, lockOwner));
      } else {
         super.trackKeysFromTransaction(modifications, lockOwner);
      }
   }

   @Override
   public synchronized void requestState(Address origin, IntSet segments) {
      DefaultIracManager defaultIracManager = asDefaultIracManager().get();
      pendingKeys.forEach((key, lockOwner) -> defaultIracManager.sendStateIfNeeded(origin, segments, key, lockOwner));
      super.requestState(origin, segments);
   }

   public synchronized void sendKeys() {
      pendingKeys.forEach(super::trackUpdatedKey);
      pendingKeys.clear();
   }

   public synchronized void enable() {
      enabled = true;
   }

   public synchronized void disable(DisableMode disableMode) {
      enabled = false;
      switch (disableMode) {
         case DROP:
            pendingKeys.clear();
            break;
         case SEND:
            sendKeys();
            break;
      }

   }

   public enum DisableMode {
      SEND,
      DROP
   }
}
