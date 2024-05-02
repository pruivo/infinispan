package org.infinispan.telemetry;

import java.util.Map;

public record InfinispanRemoteSpanContext(Map<String, String> context) implements InfinispanSpanContext {
   @Override
   public Iterable<String> keys() {
      return context.keySet();
   }

   @Override
   public String getKey(String key) {
      return context.get(key);
   }
}
