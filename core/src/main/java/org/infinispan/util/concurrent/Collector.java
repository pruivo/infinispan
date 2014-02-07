package org.infinispan.util.concurrent;

import net.jcip.annotations.ThreadSafe;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A confirmation collector. It collects confirmation and it notifies, only once, when all the confirmations has been
 * received.
 * <p/>
 * This class is Thread Safe.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@ThreadSafe
public final class Collector<T> {

   private final Set<T> confirmationPending;
   private final AtomicBoolean allConfirmed;

   public Collector(Collection<T> confirmationNeeded) {
      if (confirmationNeeded == null || confirmationNeeded.isEmpty()) {
         throw new IllegalArgumentException("Collection must be not null neither empty!");
      }
      this.confirmationPending = Collections.synchronizedSet(new HashSet<T>(confirmationNeeded));
      allConfirmed = new AtomicBoolean(false);
   }

   /**
    * It confirm the collection of the confirmation.
    *
    * @param confirmation The confirmation received.
    * @return {@code true} if the state changed, {@code false} otherwise.
    */
   public final boolean confirm(T confirmation) {
      return !allConfirmed.get() && confirmationPending.remove(confirmation);
   }

   /**
    * It updates the confirmation left. All confirmation not present in the {@param confirmationsMissing} would be
    * marked as confirmed.
    *
    * @param confirmationsMissing An update collection of confirmation needed.
    * @return {@code true} if the state changed, {@code false} otherwise.
    */
   public final boolean retain(Collection<T> confirmationsMissing) {
      return !allConfirmed.get() && confirmationPending.retainAll(confirmationsMissing);
   }

   /**
    * @return {@code true} once and only once when all the confirmation has been received. All the other invocation will
    * return {@code false}.
    */
   public final boolean isAllCollected() {
      return confirmationPending.isEmpty() && allConfirmed.compareAndSet(false, true);
   }

   /**
    * @return An unmodifiable collection of the pending confirmations.
    */
   public final Collection<T> pending() {
      return Collections.unmodifiableCollection(confirmationPending);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Collector collector = (Collector) o;

      return confirmationPending.equals(collector.confirmationPending);

   }

   @Override
   public int hashCode() {
      return confirmationPending.hashCode();
   }

   @Override
   public String toString() {
      return "Collector{confirmationPending=" + confirmationPending + '}';
   }
}
