package org.infinispan.stats.container;

/**
 * Data Access Container interface.
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
public interface DataAccessStatisticsContainer {

   void writeAccess(long duration);

   void writeAccessLockTimeout(long duration);

   void writeAccessNetworkTimeout(long duration);

   void writeAccessDeadlock(long duration);

   void writeAccessValidationError(long duration);

   void writeAccessUnknownError(long duration);

   void readAccess(long duration);

   void readAccessLockTimeout(long duration);

   void readAccessNetworkTimeout(long duration);

   void readAccessDeadlock(long duration);

   void readAccessValidationError(long duration);

   void readAccessUnknownError(long duration);


}
