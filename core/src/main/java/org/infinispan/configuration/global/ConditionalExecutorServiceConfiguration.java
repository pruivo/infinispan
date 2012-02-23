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
