package org.infinispan.interceptors.impl;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.telemetry.InfinispanTelemetry;
import org.infinispan.telemetry.SpanCategory;

public class TracingInterceptor extends DDAsyncInterceptor {

   @Inject InfinispanTelemetry telemetry;

   @Override
   protected Object handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
      var traceData = command.getTraceCommandData();
      if (traceData == null) {
         return invokeNext(ctx, command);
      }
      var suffix = ctx.isOriginLocal() ? "local" : "remote";
      var span = telemetry.startTraceRequest(command.getOperationName() + "-" + suffix, traceData.attributes(SpanCategory.CONTAINER), traceData.context());
      return invokeNextAndFinally(ctx, command, (rCtx, rCommand, rv, throwable) -> span.accept(rv, throwable));
   }
}
