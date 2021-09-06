package org.infinispan.remoting.transport.jgroups;

import org.infinispan.factories.GlobalComponentRegistry;
import org.jgroups.stack.Protocol;

public interface JGroupsProtocolVisitor {


   default void inject(GlobalComponentRegistry registry) {

   }

   default void init() {

   }

   void configureProtocol(Protocol protocol);

   default void completed() {

   }

}
