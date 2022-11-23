package org.infinispan.counter.impl.factory;

import static org.infinispan.counter.impl.CounterModuleLifecycle.JGROUPS_COUNTER_FEATURE;
import static org.infinispan.util.logging.Log.CONTAINER;

import org.infinispan.commons.util.Features;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.counter.configuration.CounterManagerConfiguration;
import org.infinispan.counter.configuration.Reliability;
import org.infinispan.counter.impl.CounterModuleLifecycle;
import org.infinispan.counter.impl.listener.CounterManagerNotificationManager;
import org.infinispan.counter.impl.manager.CounterConfigurationManager;
import org.infinispan.counter.impl.manager.CounterConfigurationStorage;
import org.infinispan.counter.impl.manager.EmbeddedCounterManager;
import org.infinispan.counter.impl.manager.PersistedCounterConfigurationStorage;
import org.infinispan.counter.impl.manager.VolatileCounterConfigurationStorage;
import org.infinispan.counter.logging.Log;
import org.infinispan.factories.AbstractComponentFactory;
import org.infinispan.factories.AutoInstantiableFactory;
import org.infinispan.factories.ComponentFactory;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.impl.ComponentAlias;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.JGroupsRaftManager;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.remoting.transport.raft.RaftManager;

/**
 * {@link ComponentFactory} for counters.
 *
 * @since 14.0
 */
@DefaultFactoryFor(classes = {
      WeakCounterFactory.class,
      StrongCounterFactory.class,
      CounterManagerNotificationManager.class,
      CounterConfigurationManager.class,
      CounterConfigurationStorage.class,
      CounterManager.class,
      EmbeddedCounterManager.class
})
@Scope(Scopes.GLOBAL)
public class CounterComponentsFactory extends AbstractComponentFactory implements AutoInstantiableFactory {

   @Inject Transport transport;

   @Override
   public Object construct(String name) {
      if (name.equals(WeakCounterFactory.class.getName())) {
         return new CacheBasedWeakCounterFactory();
      } else if (name.equals(StrongCounterFactory.class.getName())) {
         return createStrongCounterFactory();
      } else if (name.equals(CounterManagerNotificationManager.class.getName())) {
         return new CounterManagerNotificationManager();
      } else if (name.equals(CounterConfigurationManager.class.getName())) {
         return new CounterConfigurationManager(globalConfiguration);
      } else if (name.equals(CounterManager.class.getName())) {
         return new EmbeddedCounterManager();
      } else if (name.equals(EmbeddedCounterManager.class.getName())) {
         return ComponentAlias.of(CounterManager.class);
      } else if (name.equals(CounterConfigurationStorage.class.getName())) {
         return globalConfiguration.globalState().enabled() ?
               new PersistedCounterConfigurationStorage(globalConfiguration) :
               VolatileCounterConfigurationStorage.INSTANCE;
      }
      throw CONTAINER.factoryCannotConstructComponent(name);
   }

   private StrongCounterFactory createStrongCounterFactory() {
      if (!globalConfiguration.features().isAvailable(JGROUPS_COUNTER_FEATURE)) {
         Log.CONTAINER.logStrongCounterImplementation("Cache");
         return new CacheBasedStrongCounterFactory();
      }

      // JGroups counters enabled by user. Check if configuration is compatible
      if (transport == null || !(transport instanceof JGroupsTransport)) {
         String transportClass = transport == null ? "<no transport>" : transport.getClass().getSimpleName();
         throw Log.CONTAINER.transportNotCompatibleWithFeature(transportClass, Features.FEATURE_PREFIX + JGROUPS_COUNTER_FEATURE);
      }
      CounterManagerConfiguration configuration = CounterModuleLifecycle.extractConfiguration(globalConfiguration);
      if (configuration.reliability() == Reliability.AVAILABLE) {
         // No partition handling required
         Log.CONTAINER.logStrongCounterImplementation("JGroups");
         return JGroupsCounterFactory.create((JGroupsTransport) transport, configuration.numOwners());
      }

      RaftManager raftManager = transport.raftManager();
      if (!raftManager.isRaftAvailable() || !(raftManager instanceof JGroupsRaftManager)) {
         throw Log.CONTAINER.raftNotAvailableForStrongCounters();
      }
      Log.CONTAINER.logStrongCounterImplementation("JGroups Raft");
      return RaftJGroupsCounterFactory.create((JGroupsRaftManager) raftManager);
   }
}
