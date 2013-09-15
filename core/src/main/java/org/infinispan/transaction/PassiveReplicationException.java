package org.infinispan.transaction;

import org.infinispan.CacheException;

/**
 * @author Pedro Ruivo
 * @since 5.2
 */
public class PassiveReplicationException extends CacheException {
   public PassiveReplicationException(String s) {
      super(s);
   }
}
