package org.infinispan.commons.jdkspecific;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/
public class ThreadCreator {
   public static Thread createThread(ThreadGroup threadGroup, Runnable target, boolean virtual) {
      if (virtual) {
         return Thread.ofVirtual().unstarted(target);
      } else {
         return Thread.ofPlatform().group(threadGroup).unstarted(target);
      }
   }
}
