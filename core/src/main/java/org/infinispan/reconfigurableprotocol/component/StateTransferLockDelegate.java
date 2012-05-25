package org.infinispan.reconfigurableprotocol.component;

import org.infinispan.statetransfer.StateTransferLock;

/**
 * Delegates the method invocations for the correct instance depending of the protocol, for the StateTransferLock
 * component
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class StateTransferLockDelegate extends AbstractProtocolDependentComponent<StateTransferLock>
      implements StateTransferLock {

   @Override
   public void acquireExclusiveTopologyLock() {
      get().acquireExclusiveTopologyLock();
   }

   @Override
   public void releaseExclusiveTopologyLock() {
      get().releaseExclusiveTopologyLock();
   }

   @Override
   public void acquireSharedTopologyLock() {
      get().acquireSharedTopologyLock();
   }

   @Override
   public void releaseSharedTopologyLock() {
      get().releaseSharedTopologyLock();
   }

   @Override
   public void notifyTransactionDataReceived(int topologyId) {
      get().notifyTransactionDataReceived(topologyId);
   }

   @Override
   public void waitForTransactionData(int expectedTopologyId) throws InterruptedException {
      get().waitForTransactionData(expectedTopologyId);
   }

   @Override
   public void notifyTopologyInstalled(int topologyId) {
      get().notifyTopologyInstalled(topologyId);
   }

   @Override
   public void waitForTopology(int expectedTopologyId) throws InterruptedException {
      get().waitForTopology(expectedTopologyId);
   }
}
