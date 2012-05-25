package org.infinispan.reconfigurableprotocol.component;

import org.infinispan.factories.annotations.Inject;
import org.infinispan.reconfigurableprotocol.ProtocolTable;
import org.infinispan.reconfigurableprotocol.exception.AlreadyExistingComponentProtocolException;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Abstract class with the main behaviour to delegate method invocations to the correct component instance, depending of
 * the replication protocol used
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public abstract class AbstractProtocolDependentComponent<T> {

   private final ConcurrentMap<String, T> protocolDependentComponent = new ConcurrentHashMap<String, T>();
   private ProtocolTable protocolTable;

   @Inject
   public final void inject(ProtocolTable protocolTable) {
      this.protocolTable = protocolTable;
   }

   /**
    * adds a new component for a replication protocol
    *
    * @param protocolId the protocol Id
    * @param component  the component
    * @throws AlreadyExistingComponentProtocolException
    *          if the protocol already has a component
    */
   public final void add(String protocolId, T component) throws AlreadyExistingComponentProtocolException {
      if (protocolDependentComponent.putIfAbsent(protocolId, component) != null) {
         throw new AlreadyExistingComponentProtocolException("Protocol " + protocolId + " has already a component register");
      }
   }

   /**
    * returns the component depending of the current protocol Id
    *
    * @return the component depending of the current protocol Id
    */
   public final T get() {
      return protocolDependentComponent.get(protocolTable.getThreadProtocolId());
   }
}
