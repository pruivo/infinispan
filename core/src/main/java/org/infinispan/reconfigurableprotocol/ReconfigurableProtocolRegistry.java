package org.infinispan.reconfigurableprotocol;

import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.reconfigurableprotocol.exception.AlreadyRegisterProtocolException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * class responsible to keep all the possible ReconfigurableProtocol for this cache. It manages internally the Id of
 * each protocol
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class ReconfigurableProtocolRegistry {

   private static final Log log = LogFactory.getLog(ReconfigurableProtocolRegistry.class);
   private final Map<String, ReconfigurableProtocol> idsToProtocol;
   private InterceptorChain interceptorChain;

   public ReconfigurableProtocolRegistry() {
      this.idsToProtocol = new ConcurrentHashMap<String, ReconfigurableProtocol>();
   }

   /**
    * injects the interceptor chain in order to add new replication protocols
    *
    * @param interceptorChain the interceptor chain
    */
   public final void inject(InterceptorChain interceptorChain) {
      this.interceptorChain = interceptorChain;
   }

   /**
    * returns the current ids and replication protocols currently register
    *
    * @return the current ids and replication protocols currently register
    */
   public final Collection<ReconfigurableProtocol> getAllAvailableProtocols() {
      return Collections.unmodifiableCollection(idsToProtocol.values());
   }

   /**
    * registers a new protocol to this registry and set it an id
    *
    * @param protocol the new protocol
    * @throws AlreadyRegisterProtocolException
    *          if the protocol is already register
    */
   public final synchronized void registerNewProtocol(ReconfigurableProtocol protocol)
         throws AlreadyRegisterProtocolException {

      if (protocol == null) {
         log.warn("Tried to register a new replication protocol, but it is null");
         throw new NullPointerException("Trying to register a null protocol");
      } else if (idsToProtocol.containsKey(protocol.getUniqueProtocolName())) {
         log.warnf("Tried to register a new replication protocol but it is already register. Protocol is %s",
                   protocol.getUniqueProtocolName());
         throw new AlreadyRegisterProtocolException(protocol);
      }

      idsToProtocol.put(protocol.getUniqueProtocolName(), protocol);
      protocol.bootstrapProtocol();
      interceptorChain.registerNewProtocol(protocol);

      if (log.isDebugEnabled()) {
         log.debugf("Register successfully the new replication protocol %s", protocol.getUniqueProtocolName());
      }
   }

   /**
    * returns the protocol associated to this protocol id
    *
    * @param protocolId the protocol id
    * @return the reconfigurable protocol instance or null if the protocol id does not exists
    */
   public final ReconfigurableProtocol getProtocolById(String protocolId) {
      return idsToProtocol.get(protocolId);
   }
}
