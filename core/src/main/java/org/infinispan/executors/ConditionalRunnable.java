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
package org.infinispan.executors;

/**
 * A special Runnable (for the particular case of Total Order) that is only set to a thread when it is ready to
 * be executed without blocking the thread
 *
 * Use case:
 *    - in Total Order, when the prepare is delivered, the runnable blocks waiting for the previous conflicting
 *    transactions to be finished. In a normal executor service, this will take a thread and that thread will be
 *    blocked. This way, the runnable waits on the queue and not in the Thread
 *
 * Used in {@code ConditionalExecutorService}
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public interface ConditionalRunnable extends Runnable {

   /**
    * @return  true if this Runnable is ready to be executed without blocking
    */
   boolean isReady();

}
