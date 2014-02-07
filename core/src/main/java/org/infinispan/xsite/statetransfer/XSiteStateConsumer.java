package org.infinispan.xsite.statetransfer;

/**
 * It contains the logic needed to consume the state sent from other site.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public interface XSiteStateConsumer {

   /**
    * It notifies the start of state transfer from other site.
    */
   public void startStateTransfer();

   /**
    * It notifies the end of state transfer from other site.
    */
   public void endStateTransfer();

   /**
    * It applies state from other site. The state is applied as a normal operation in this cluster, so make sure that
    * this method is not invoked twice or more for the state state.
    *
    * @param chunk a chunk of keys
    * @throws Exception if something go wrong while applying the state
    */
   public void applyState(XSiteState[] chunk) throws Exception;
}
