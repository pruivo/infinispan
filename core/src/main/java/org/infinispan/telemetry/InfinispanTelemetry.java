package org.infinispan.telemetry;

import org.infinispan.commands.TracedCommand;
import org.infinispan.telemetry.impl.DisabledInfinispanSpan;

public interface InfinispanTelemetry {

   <T> InfinispanSpan<T> startTraceRequest(String operationName, InfinispanSpanAttributes attributes);

   <T> InfinispanSpan<T> startTraceRequest(String operationName, InfinispanSpanAttributes attributes, InfinispanSpanContext context);

   default <T> InfinispanSpan<T> startRemoteTraceRequest(TracedCommand command, SpanCategory category) {
      var data = command.getTraceCommandData();
      if (data == null || data.context() == null) {
         return DisabledInfinispanSpan.instance();
      }
      return startTraceRequest(command.getOperationName() + "-transport", data.attributes(category), data.context());
   }

   default InfinispanRemoteSpanContext createRemoteContext() {
      return null;
   }

   void setNodeName(String nodeName);

   <T> InfinispanSpan<T> currentSpan();
}
