package org.infinispan.container.versioning.gmu;

import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.dataplacement.ClusterSnapshot;
import org.infinispan.remoting.transport.Address;

import java.util.Collection;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public interface GMUVersionGenerator extends VersionGenerator {
   GMUVersion mergeAndMax(EntryVersion... entryVersions);

   GMUVersion mergeAndMin(EntryVersion... entryVersions);

   GMUVersion calculateCommitVersion(EntryVersion prepareVersion, Collection<Address> affectedOwners);

   GMUCacheEntryVersion convertVersionToWrite(EntryVersion version, int subVersion);

   GMUReadVersion convertVersionToRead(EntryVersion version);

   GMUVersion calculateMaxVersionToRead(EntryVersion transactionVersion, Collection<Address> alreadyReadFrom);

   GMUVersion calculateMinVersionToRead(EntryVersion transactionVersion, Collection<Address> alreadyReadFrom);

   GMUVersion setNodeVersion(EntryVersion version, long value);

   GMUVersion updatedVersion(EntryVersion entryVersion);

   ClusterSnapshot getClusterSnapshot(int viewId);

   Address getAddress();
}
