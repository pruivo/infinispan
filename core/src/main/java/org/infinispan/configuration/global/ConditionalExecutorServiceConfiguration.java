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
package org.infinispan.configuration.global;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class ConditionalExecutorServiceConfiguration {

   private final int corePoolSize;
   private final int maxPoolSize;
   private final int threadPriority;
   private final long keepAliveTime; //milliseconds
   private final int queueSize;

   public ConditionalExecutorServiceConfiguration(int corePoolSize, int maxPoolSize, int threadPriority,
                                                  long keepAliveTime, int queueSize) {
      this.corePoolSize = corePoolSize;
      this.maxPoolSize = maxPoolSize;
      this.threadPriority = threadPriority;
      this.keepAliveTime = keepAliveTime;
      this.queueSize = queueSize;
   }

   public int corePoolSize() {
      return corePoolSize;
   }

   public int maxPoolSize() {
      return maxPoolSize;
   }

   public int threadPriority() {
      return threadPriority;
   }

   public long keepAliveTime() {
      return keepAliveTime;
   }

   public int queueSize() {
      return queueSize;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      ConditionalExecutorServiceConfiguration that = (ConditionalExecutorServiceConfiguration) o;

      if (corePoolSize != that.corePoolSize) return false;
      if (keepAliveTime != that.keepAliveTime) return false;
      if (maxPoolSize != that.maxPoolSize) return false;
      if (queueSize != that.queueSize) return false;
      if (threadPriority != that.threadPriority) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = corePoolSize;
      result = 31 * result + maxPoolSize;
      result = 31 * result + threadPriority;
      result = 31 * result + (int) (keepAliveTime ^ (keepAliveTime >>> 32));
      result = 31 * result + queueSize;
      return result;
   }

   @Override
   public String toString() {
      return "ConditionalExecutorServiceConfiguration{" +
            "corePoolSize=" + corePoolSize +
            ", maxPoolSize=" + maxPoolSize +
            ", threadPriority=" + threadPriority +
            ", keepAliveTime=" + keepAliveTime +
            ", queueSize=" + queueSize +
            '}';
   }
}
