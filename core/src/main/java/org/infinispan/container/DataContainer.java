package org.infinispan.container;

import org.infinispan.commons.util.concurrent.ParallelIterableMap;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.metadata.Metadata;
import org.infinispan.persistence.spi.AdvancedCacheLoader;

import java.util.Collection;
import java.util.Set;

/**
 * The main internal data structure which stores entries
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Galder Zamarreño
 * @author Vladimir Blagojevic
 * @author Pedro Ruivo
 * @since 4.0
 */
@Scope(Scopes.NAMED_CACHE)
public interface DataContainer extends Iterable<InternalCacheEntry> {

   /**
    * Retrieves a cached entry from container or from cache loader (a.k.a. persistence).
    * <p/>
    * The {@link DataContainer.AccessMode} has the following effect:
    * <p/>
    * <li> <ul> {@link DataContainer.AccessMode#SKIP_CONTAINER} - returns the entry from
    * cache loader; </ul> <ul> {@link DataContainer.AccessMode#SKIP_PERSISTENCE} - returns
    * the entry from container; </ul> <ul> {@link DataContainer.AccessMode#ALL} - returns the
    * entry from container. If it does not exists, it tries to load it from cache loader and stores it in container,
    * atomically (the entry must be activated)</ul> </li>
    *
    * @param k    key under which entry is stored
    * @param mode the access mode which behavior is described above
    * @return entry, if it exists and has not expired, or {@code null} if not
    * @throws java.lang.IllegalArgumentException if {@code mode} is not valid.
    * @throws java.lang.NullPointerException     if any parameter is null.
    */
   InternalCacheEntry get(Object k, AccessMode mode);

   /**
    * Retrieves a cache entry in the same way as {@link #get(Object, DataContainer.AccessMode)}
    * except that it does not update or reorder any of the internal constructs. I.e., expiration does not happen, and in
    * the case of the LRU container, the entry is not moved to the end of the chain.
    * <p/>
    * This method should be used instead of {@link #get(Object, DataContainer.AccessMode)}
    * when called while iterating through the data container using methods like {@link
    * #keySet(DataContainer.AccessMode)} to avoid changing the underlying collection's
    * order.
    * <p/>
    * The {@link DataContainer.AccessMode} has the following effect:
    * <p/>
    * <li> <ul> {@link DataContainer.AccessMode#SKIP_CONTAINER} - returns the entry from
    * cache loader; </ul> <ul> {@link DataContainer.AccessMode#SKIP_PERSISTENCE} - returns
    * the entry from container; </ul> <ul> {@link DataContainer.AccessMode#ALL} - returns the
    * entry from container. If it does not exists, it tries to load it from cache loader (note that the entry is not
    * stored in container as happens in {@link #get(Object, DataContainer.AccessMode)}</ul>
    * </li>
    *
    * @param k    key under which entry is stored
    * @param mode the access mode
    * @return entry, if it exists, or null if not
    * @throws java.lang.IllegalArgumentException if {@code mode} is not valid.
    * @throws java.lang.NullPointerException     if any parameter is null.
    */
   InternalCacheEntry peek(Object k, AccessMode mode);

   /**
    * Stores an entry in the container and/or cache writer along with metadata adding information such lifespan of
    * entry, max idle time, version information...etc.
    * <p/>
    * The {@link DataContainer.AccessMode} has the following effect:
    * <p/>
    * <li> <ul> {@link DataContainer.AccessMode#SKIP_CONTAINER} - stores only in cache store;
    * </ul> <ul> {@link DataContainer.AccessMode#SKIP_PERSISTENCE} - stores only in
    * container; </ul> <ul> {@link DataContainer.AccessMode#ALL} - store in container and in
    * cache writer.</ul> </li>
    *
    * @param k        key under which to store entry
    * @param v        value to store
    * @param metadata metadata of the entry
    */
   void put(Object k, Object v, Metadata metadata);

   /**
    * Tests whether an entry exists in the container and/or cache loader.
    * <p/>
    * The {@link DataContainer.AccessMode} has the following effect:
    * <p/>
    * <li> <ul> {@link DataContainer.AccessMode#SKIP_CONTAINER} - checks only in cache
    * loader; </ul> <ul> {@link DataContainer.AccessMode#SKIP_PERSISTENCE} - checks only in
    * container; </ul> <ul> {@link DataContainer.AccessMode#ALL} - checks in container and in
    * cache loader.</ul> </li>
    *
    * @param k    key to test
    * @param mode the access mode.
    * @return true if entry exists and has not expired; false otherwise
    * @throws java.lang.IllegalArgumentException if {@code mode} is not valid.
    * @throws java.lang.NullPointerException     if any parameter is null.
    */
   boolean containsKey(Object k, AccessMode mode);

   /**
    * Removes an entry from the container and/or cache loader.
    * <p/>
    * The {@link DataContainer.AccessMode} has the following effect:
    * <p/>
    * <li> <ul> {@link DataContainer.AccessMode#SKIP_CONTAINER} - removes only from cache
    * writer </ul> <ul> {@link DataContainer.AccessMode#SKIP_PERSISTENCE} - this remove
    * behaves as eviction. Atomically, the entry is removed from container and stored in cache writer (the key must be
    * passivated); </ul> <ul> {@link DataContainer.AccessMode#ALL} - removes from container
    * and cache writer.</ul> </li>
    *
    * @param k key to remove
    * @return entry removed, or null if it didn't exist or had expired
    * @throws java.lang.IllegalArgumentException if {@code mode} is not valid.
    * @throws java.lang.NullPointerException     if any parameter is null.
    */
   InternalCacheEntry remove(Object k, AccessMode mode);

   /**
    * The {@link DataContainer.AccessMode} has the following effect:
    * <p/>
    * <li> <ul> {@link DataContainer.AccessMode#SKIP_CONTAINER} - size only from cache loader
    * </ul> <ul> {@link DataContainer.AccessMode#SKIP_PERSISTENCE} - size only from
    * container; </ul> <ul> {@link DataContainer.AccessMode#ALL} - size from container and
    * cache loader.</ul> </li>
    *
    * @return count of the number of entries in the container and/or persistence depending of the {@link
    * DataContainer.AccessMode}
    * @throws java.lang.IllegalArgumentException if {@code mode} is not valid.
    * @throws java.lang.NullPointerException     if any parameter is null.
    */
   int size(AccessMode mode);

   /**
    * Removes all entries in the container
    */
   @Stop(priority = 999)
   void clear();

   /**
    * Returns a set of keys in the container and/or persistence depending of the {@link
    * DataContainer.AccessMode}. When iterating through the container using this method,
    * clients should never call {@link #get(Object, DataContainer.AccessMode)} method but
    * instead {@link #peek(Object, DataContainer.AccessMode)}, in order to avoid changing the
    * order of the underlying collection as a side of effect of iterating through it.
    *
    * @return a set of keys
    * @throws java.lang.IllegalArgumentException if {@code mode} is not valid.
    * @throws java.lang.NullPointerException     if any parameter is null.
    */
   Set<Object> keySet(AccessMode mode);

   /**
    * @return a set of values contained in the container and/or persistence depending on the {@link
    * DataContainer.AccessMode}
    * @throws java.lang.IllegalArgumentException if {@code mode} is not valid.
    * @throws java.lang.NullPointerException     if any parameter is null.
    */
   Collection<Object> values(AccessMode mode);

   /**
    * Returns a mutable set of immutable cache entries exposed as immutable Map.Entry instances. Clients of this method
    * such as Cache.entrySet() operation implementors are free to convert the set into an immutable set if needed, which
    * is the most common use case.
    * <p/>
    * If a client needs to iterate through a mutable set of mutable cache entries, it should iterate the container
    * itself rather than iterating through the return of entrySet().
    *
    * @return a set of immutable cache entries
    * @throws java.lang.IllegalArgumentException if {@code mode} is not valid.
    * @throws java.lang.NullPointerException     if any parameter is null.
    */
   Set<InternalCacheEntry> entrySet(AccessMode mode);

   /**
    * Purges entries that have passed their expiry time from container
    */
   void purgeExpired();

   /**
    * Executes task specified by the given action on the container key/values filtered using the specified key filter.
    *
    * @param filter the filter for the container key/values
    * @param action the specified action to execute on filtered key/values
    * @throws InterruptedException
    */
   public void executeTask(AdvancedCacheLoader.KeyFilter<Object> filter,
                           ParallelIterableMap.KeyValueAction<Object, InternalCacheEntry> action) throws InterruptedException;

   public static enum AccessMode {
      SKIP_PERSISTENCE, SKIP_CONTAINER, ALL
   }
}
