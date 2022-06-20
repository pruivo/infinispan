package org.infinispan.container.versioning.irac;

/**
 * No-op implementation for {@link IracTombstoneManager}.
 * <p>
 * It is used when IRAC is not enabled.
 *
 * @since 14.0
 */
public final class NoOpIracTombstoneManager implements IracTombstoneManager {

   private static final NoOpIracTombstoneManager INSTANCE = new NoOpIracTombstoneManager();

   private NoOpIracTombstoneManager() {
   }

   public static NoOpIracTombstoneManager getInstance() {
      return INSTANCE;
   }

   @Override
   public int size() {
      return 0;
   }

   @Override
   public boolean isTaskRunning() {
      return false;
   }

   @Override
   public long getCurrentDelayMillis() {
      return 0;
   }

}
