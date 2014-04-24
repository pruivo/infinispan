package org.infinispan.util.concurrent.locks.containers.wrappers;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class LockHolder {

   private static final Log log = LogFactory.getLog(LockHolder.class);
   private static final boolean trace = log.isTraceEnabled();
   private final Object owner;
   private final Object key;
   private final Collection<LockHolderListener> listeners;
   private volatile boolean ready;

   public LockHolder(Object owner, Object key) {
      this.owner = owner;
      this.key = key;
      this.ready = false;
      listeners = new CopyOnWriteArrayList<LockHolderListener>();
   }

   public final void addListener(LockHolderListener listener) {
      if (trace) {
         log.tracef("Adding listener '%s' to %s", listener, toString());
      }
      listeners.add(listener);
   }

   public final void markReady() {
      if (trace) {
         log.tracef("Mark this '%s' as ready!", toString());
      }
      this.ready = true;
      for (LockHolderListener listener : listeners) {
         listener.notifyAcquire();
      }
   }

   public final boolean isReady() {
      return ready;
   }

   public final Object getOwner() {
      return owner;
   }

   public final Object getKey() {
      return key;
   }

   @Override
   public String toString() {
      return "LockHolder{" +
            "owner=" + owner +
            ", key=" + key +
            ", ready=" + ready +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      LockHolder that = (LockHolder) o;

      if (ready != that.ready) return false;
      if (!key.equals(that.key)) return false;
      if (!owner.equals(that.owner)) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = owner.hashCode();
      result = 31 * result + key.hashCode();
      result = 31 * result + (ready ? 1 : 0);
      return result;
   }

   public static interface LockHolderListener {
      void notifyAcquire();
   }
}
