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
package org.infinispan.transaction;

import org.infinispan.transaction.xa.GlobalTransaction;

import java.util.concurrent.CountDownLatch;

/**
 * This latch represents the dependencies between the transactions. A transaction has a set of this latch and
 * it will wait on each one. The latch is released when the transaction finishes (ie, the modification are applied or
 * it is rollbacked)
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class TxDependencyLatch extends CountDownLatch {
   private String globalTransaction;

   public TxDependencyLatch(GlobalTransaction globalTransaction) {
      super(1);
      this.globalTransaction = globalTransaction.prettyPrint();
   }

   @Override
   public String toString() {
      return "TxDependencyLatch{" +
            "globalTransaction='" + globalTransaction + '\'' +
            '}';
   }
}
