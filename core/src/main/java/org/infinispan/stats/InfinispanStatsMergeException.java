package org.infinispan.stats;

/**
 * Websiste: www.cloudtm.eu
 * Date: 20/04/12
 * @author Diego Didona <didona@gsd.inesc-id.pt>
 * @since 5.2
 */
public class InfinispanStatsMergeException extends Exception {

   public InfinispanStatsMergeException() {
   }

   public InfinispanStatsMergeException(String s) {
      super(s);
   }

   public InfinispanStatsMergeException(String s, Throwable throwable) {
      super(s, throwable);
   }

   public InfinispanStatsMergeException(Throwable throwable) {
      super(throwable);
   }
}
