package org.infinispan.remoting.transport.jgroups;

import org.infinispan.commons.util.ServiceFinder;
import org.infinispan.factories.AbstractComponentFactory;
import org.infinispan.factories.AutoInstantiableFactory;
import org.infinispan.factories.annotations.DefaultFactoryFor;

@DefaultFactoryFor(classes = CompositeJGroupsProtocolVisitor.class)
public class JGroupsProtocolVisitorFactory extends AbstractComponentFactory implements AutoInstantiableFactory {

   @Override
   public Object construct(String name) {
      log.warn("JGroupsProtocolVisitorFactory");
      CompositeJGroupsProtocolVisitor visitor = new CompositeJGroupsProtocolVisitor(globalComponentRegistry);
      for (JGroupsProtocolVisitor v : ServiceFinder.load(JGroupsProtocolVisitor.class, globalConfiguration.classLoader())) {
         log.warnf("Found: %s", v);
         visitor.add(v);
      }
      return visitor;
   }
}
