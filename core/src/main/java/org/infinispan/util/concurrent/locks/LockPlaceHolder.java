package org.infinispan.util.concurrent.locks;

import net.jcip.annotations.ThreadSafe;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@ThreadSafe
public interface LockPlaceHolder {

   boolean isReady();

   void awaitReady() throws InterruptedException;

}
