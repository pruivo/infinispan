package org.infinispan.transaction.gmu;

import org.infinispan.CacheException;

/**
 * @author Pedro Ruivo
 * @author Sebastiano Peluso
 * @since 5.2
 */
public class ValidationException extends CacheException {

   private static final String UNKNOWN_KEY = "UNKOWN";

   private final Object key;

   public ValidationException() {
      super();
      this.key = UNKNOWN_KEY;
   }

   public ValidationException(String msg, Object key) {
      super(msg);
      this.key = key;
   }

   public ValidationException(Throwable throwable) {
      super(throwable.getMessage());
      if (throwable instanceof ValidationException) {
         this.key = ((ValidationException) throwable).getKey();
      } else {
         this.key = UNKNOWN_KEY;
      }
   }

   public Object getKey() {
      return key;
   }
}
