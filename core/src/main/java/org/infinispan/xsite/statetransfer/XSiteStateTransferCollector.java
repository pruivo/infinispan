package org.infinispan.xsite.statetransfer;

import org.infinispan.remoting.transport.Address;
import org.infinispan.util.concurrent.Collector;

import java.util.Collection;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public final class XSiteStateTransferCollector {

   private final Collector<Address> collector;

   public XSiteStateTransferCollector(Collection<Address> confirmationPending) {
      this.collector = new Collector<Address>(confirmationPending);
   }

   public boolean confirmStateTransfer(Address node) {
      collector.confirm(node);
      return collector.isAllCollected();
   }

   public boolean updateMembers(Collection<Address> members) {
      collector.retain(members);
      return collector.isAllCollected();
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      XSiteStateTransferCollector that = (XSiteStateTransferCollector) o;

      return collector.equals(that.collector);

   }

   @Override
   public int hashCode() {
      return collector.hashCode();
   }

   @Override
   public String toString() {
      return "XSiteStateTransferCollector{" +
            "collector=" + collector +
            '}';
   }
}
