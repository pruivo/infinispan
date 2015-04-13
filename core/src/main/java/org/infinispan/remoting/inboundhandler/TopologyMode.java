package org.infinispan.remoting.inboundhandler;

import org.infinispan.statetransfer.StateTransferLock;

import java.util.concurrent.TimeUnit;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public enum TopologyMode {
   WAIT_TOPOLOGY {
      @Override
      void await(StateTransferLock stateTransferLock, int topologyId) throws InterruptedException {
         stateTransferLock.waitForTopology(topologyId, 1, TimeUnit.DAYS);
      }

      @Override
      boolean isReady(StateTransferLock stateTransferLock, int topologyId) {
         return true;
      }
   },
   READY_TOPOLOGY {
      @Override
      void await(StateTransferLock stateTransferLock, int topologyId) throws InterruptedException {/*no-op*/}

      @Override
      boolean isReady(StateTransferLock stateTransferLock, int topologyId) {
         return stateTransferLock.topologyReceived(topologyId);
      }
   },
   WAIT_TX_DATA {
      @Override
      void await(StateTransferLock stateTransferLock, int topologyId) throws InterruptedException {
         stateTransferLock.waitForTransactionData(topologyId, 1, TimeUnit.DAYS);
      }

      @Override
      boolean isReady(StateTransferLock stateTransferLock, int topologyId) {
         return true;
      }
   },
   READY_TX_DATA {
      @Override
      void await(StateTransferLock stateTransferLock, int topologyId) throws InterruptedException {/*no-op*/}

      @Override
      boolean isReady(StateTransferLock stateTransferLock, int topologyId) {
         return stateTransferLock.transactionDataReceived(topologyId);
      }
   };

   abstract void await(StateTransferLock stateTransferLock, int topologyId) throws InterruptedException;

   abstract boolean isReady(StateTransferLock stateTransferLock, int topologyId);

   public static TopologyMode create(boolean onExecutor, boolean txData) {
      if (onExecutor && txData) {
         return TopologyMode.READY_TX_DATA;
      } else if (onExecutor) {
         return TopologyMode.READY_TOPOLOGY;
      } else if (txData) {
         return TopologyMode.WAIT_TX_DATA;
      } else {
         return TopologyMode.WAIT_TOPOLOGY;
      }
   }
}
