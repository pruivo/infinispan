package org.infinispan.remoting.transport.jgroups;

import java.util.HashSet;
import java.util.Set;

import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.jgroups.stack.Protocol;

@Scope(Scopes.GLOBAL)
public class CompositeJGroupsProtocolVisitor implements JGroupsProtocolVisitor {

   private final Set<JGroupsProtocolVisitor> visitors;
   private final GlobalComponentRegistry registry;

   @Start
   public void injectRegistry() {
      visitors.forEach(v -> v.inject(registry));
   }

   public CompositeJGroupsProtocolVisitor(GlobalComponentRegistry registry) {
      this.registry = registry;
      this.visitors = new HashSet<>();
   }

   @Override
   public void init() {
      visitors.forEach(JGroupsProtocolVisitor::init);
   }

   @Override
   public void configureProtocol(Protocol protocol) {
      visitors.forEach(v -> v.configureProtocol(protocol));
   }

   @Override
   public void completed() {
      visitors.forEach(JGroupsProtocolVisitor::completed);
   }

   public void add(JGroupsProtocolVisitor visitor) {
      visitors.add(visitor);
   }
}
