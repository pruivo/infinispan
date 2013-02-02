/*
 * INESC-ID, Instituto de Engenharia de Sistemas e Computadores Investigação e Desevolvimento em Lisboa
 * Copyright 2013 INESC-ID and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 3.0 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
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
