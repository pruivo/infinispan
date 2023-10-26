package org.infinispan.server.core.telemetry;

import static org.infinispan.util.logging.Log.CONTAINER;

import java.util.Collections;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.factories.AbstractComponentFactory;
import org.infinispan.factories.AutoInstantiableFactory;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.telemetry.InfinispanTelemetry;
import org.infinispan.telemetry.impl.DisabledInfinispanTelemetry;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;

@Scope(Scopes.GLOBAL)
@DefaultFactoryFor(classes = InfinispanTelemetry.class)
public class TelemetryServiceFactory extends AbstractComponentFactory implements AutoInstantiableFactory {

   private static final Log log = LogFactory.getLog(TelemetryServiceFactory.class);

   private static final boolean TRACING_ENABLED = Boolean.getBoolean("infinispan.tracing.enabled");

   @Override
   public Object construct(String componentName) {
      if (!componentName.equals(InfinispanTelemetry.class.getName())) {
         throw CONTAINER.factoryCannotConstructComponent(componentName);
      }
      if (!TRACING_ENABLED) {
         return new DisabledInfinispanTelemetry();
      }

      try {
         OpenTelemetry openTelemetry = AutoConfiguredOpenTelemetrySdk.builder()
               // At the moment we don't export Infinispan server metrics with OpenTelemetry,
               // so we manually disable any metrics export
               .addPropertiesSupplier(() -> Collections.singletonMap("otel.metrics.exporter", "none"))
               .build()
               .getOpenTelemetrySdk();

         log.telemetryLoaded(openTelemetry);
         return new OpenTelemetryService(openTelemetry);
      } catch (Throwable e) {
         log.errorOnLoadingTelemetry();
         return new DisabledInfinispanTelemetry();
      }
   }
}
