package org.infinispan.stats;

/**
 * Websiste: www.cloudtm.eu
 * Date: 02/05/12
 * @author Diego Didona <didona@gsd.inesc-id.pt>
 * @since 5.2
 */
public class NoIspnStatException extends RuntimeException {

   public NoIspnStatException() {
   }

   public NoIspnStatException(String s) {
      super(s);
   }

   public NoIspnStatException(String s, Throwable throwable) {
      super(s, throwable);
   }

   public NoIspnStatException(Throwable throwable) {
      super(throwable);
   }
}
