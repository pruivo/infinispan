package org.infinispan.commands;

import org.infinispan.telemetry.InfinispanRemoteSpanContext;
import org.infinispan.telemetry.InfinispanSpanAttributes;
import org.infinispan.telemetry.SpanCategory;
import org.infinispan.telemetry.impl.CacheSpanAttribute;

public interface TracedCommand {

   default String getOperationName() {
      return getClass().getSimpleName();
   }

   default void setTraceCommandData(TraceCommandData data) {
      //no-op
   }

   default TraceCommandData getTraceCommandData() {
      return null;
   }

   default void updateTraceData(CacheSpanAttribute cacheSpanAttribute) {
      var traceData = getTraceCommandData();
      if (traceData == null) {
         return;
      }
      setTraceCommandData(new TraceCommandData(cacheSpanAttribute, traceData.context));
   }

   record TraceCommandData(CacheSpanAttribute cacheSpanAttribute, InfinispanRemoteSpanContext context) {

      public InfinispanSpanAttributes attributes(SpanCategory category) {
         return cacheSpanAttribute == null ? null : cacheSpanAttribute.getAttributes(category);
      }

   }

}
