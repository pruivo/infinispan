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

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 4.0
 */
public class ConditionalExecutorServiceConfigurationBuilder
      extends AbstractGlobalConfigurationBuilder<ConditionalExecutorServiceConfiguration> {

   private static final Log log = LogFactory.getLog(ConditionalExecutorServiceConfigurationBuilder.class);
   private int corePoolSize = 1;
   private int maxPoolSize = 2;
   private int threadPriority = Thread.NORM_PRIORITY;
   private long keepAliveTime = 60000;
   private int queueSize = 10000;

   public ConditionalExecutorServiceConfigurationBuilder(GlobalConfigurationBuilder globalConfig) {
      super(globalConfig);
   }

   public ConditionalExecutorServiceConfigurationBuilder corePoolSize(int corePoolSize) {
      this.corePoolSize = corePoolSize;
      return this;
   }

   public ConditionalExecutorServiceConfigurationBuilder maxPoolSize(int maxPoolSize) {
      this.maxPoolSize = maxPoolSize;
      return this;
   }

   public ConditionalExecutorServiceConfigurationBuilder threadPriority(int threadPriority) {
      this.threadPriority = threadPriority;
      return this;
   }

   public ConditionalExecutorServiceConfigurationBuilder keepAliveTime(long keepAliveTime) {
      this.keepAliveTime = keepAliveTime;
      return this;
   }

   public ConditionalExecutorServiceConfigurationBuilder queueSize(int queueSize) {
      this.queueSize = queueSize;
      return this;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      ConditionalExecutorServiceConfigurationBuilder that = (ConditionalExecutorServiceConfigurationBuilder) o;

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
      return "ConditionalExecutorServiceConfigurationBuilder{" +
            "corePoolSize=" + corePoolSize +
            ", maxPoolSize=" + maxPoolSize +
            ", threadPriority=" + threadPriority +
            ", keepAliveTime=" + keepAliveTime +
            ", queueSize=" + queueSize +
            '}';
   }

   @Override
   protected GlobalConfigurationChildBuilder read(ConditionalExecutorServiceConfiguration template) {
      this.corePoolSize = template.corePoolSize();
      this.maxPoolSize = template.maxPoolSize();
      this.threadPriority = template.threadPriority();
      this.keepAliveTime = template.keepAliveTime();
      this.queueSize = template.queueSize();
      return this;
   }

   @Override
   void validate() {
      if (corePoolSize <= 0) {
         log.error("Core Pool Size should be higher than zero. Setting value to 1");
         corePoolSize = 1;
      }
      if (maxPoolSize < corePoolSize) {
         log.errorf("Max Pool Size should be higher or equal than Core Pool Siz. Setting value to %s", corePoolSize);
         maxPoolSize = corePoolSize;
      }
      if (threadPriority < Thread.MIN_PRIORITY) {
         log.error("Thread Priority should higher than Thread.MIN_PRIORITY. Setting value to Thread.MIN_PRIORITY");
         threadPriority = Thread.MIN_PRIORITY;
      }
      if (threadPriority > Thread.MAX_PRIORITY) {
         log.error("Thread Priority should lower than Thread.MAX_PRIORITY. Setting value to Thread.MAX_PRIORITY");
         threadPriority = Thread.MAX_PRIORITY;
      }
      if (keepAliveTime <= 0) {
         log.error("Keep Alive Time should be higher than zero. Setting value to 1000 milliseconds");
         keepAliveTime = 1000;
      }
      if (queueSize <= 0) {
         log.error("Queue Size should be higher than zero. Setting value to 10");
         queueSize = 10;
      }
   }

   @Override
   ConditionalExecutorServiceConfiguration create() {
      return new ConditionalExecutorServiceConfiguration(corePoolSize, maxPoolSize, threadPriority, keepAliveTime,
                                                         queueSize);
   }
}


