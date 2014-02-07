package org.infinispan.xsite.statetransfer;

import org.infinispan.remoting.transport.Address;

import java.util.Collection;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class XSiteProviderDelegate implements XSiteStateProvider {

   private final XSiteStateProvider xSiteStateProvider;

   public XSiteProviderDelegate(XSiteStateProvider xSiteStateProvider) {
      this.xSiteStateProvider = xSiteStateProvider;
   }

   @Override
   public void startStateTransfer(String siteName, Address requestor) {
      xSiteStateProvider.startStateTransfer(siteName, requestor);
   }

   @Override
   public void cancelStateTransfer(String siteName) {
      xSiteStateProvider.cancelStateTransfer(siteName);
   }

   @Override
   public Collection<String> getCurrentStateSending() {
      return xSiteStateProvider.getCurrentStateSending();
   }
}
