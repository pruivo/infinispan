/*
 * INESC-ID, Instituto de Engenharia de Sistemas e Computadores Investigação e Desevolvimento em Lisboa
 * Copyright 2013 INESC-ID and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 3.0 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.interceptors.gmu;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.read.AbstractDataCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.container.entries.gmu.InternalGMUCacheEntry;
import org.infinispan.container.entries.gmu.InternalGMUCacheValue;
import org.infinispan.container.gmu.L1GMUContainer;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.container.versioning.gmu.GMUVersion;
import org.infinispan.container.versioning.gmu.GMUVersionGenerator;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.SingleKeyNonTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DataLocality;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.distribution.TxDistributionInterceptor;
import org.infinispan.remoting.responses.ClusteredGetResponseValidityFilter;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.StateConsumer;
import org.infinispan.transaction.gmu.CommitLog;
import org.infinispan.transaction.gmu.VersionNotAvailableException;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

import static org.infinispan.transaction.gmu.GMUHelper.joinAndSetTransactionVersion;
import static org.infinispan.transaction.gmu.GMUHelper.toAlreadyReadFromMask;

/**
 * @author Pedro Ruivo
 * @author Sebastiano Peluso
 * @author Hugo Pimentel
 * @since 5.2
 */
public class GMUDistributionInterceptor extends TxDistributionInterceptor {

   private static final Log log = LogFactory.getLog(GMUDistributionInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();
   protected GMUVersionGenerator versionGenerator;
   private L1GMUContainer l1GMUContainer;
   private CommitLog commitLog;
   private StateConsumer stateConsumer;

   @Inject
   public void setVersionGenerator(VersionGenerator versionGenerator, L1GMUContainer l1GMUContainer, CommitLog commitLog, StateConsumer stateConsumer) {
      this.versionGenerator = (GMUVersionGenerator) versionGenerator;
      this.l1GMUContainer = l1GMUContainer;
      this.commitLog = commitLog;
      this.stateConsumer = stateConsumer;
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      Object retVal = invokeNextInterceptor(ctx, command);
      if (shouldInvokeRemoteTxCommand(ctx)) {
         Future<?> f = flushL1Caches(ctx);
         retVal = sendGMUCommitCommand(retVal, command, getCommitNodes(ctx));
         blockOnL1FutureIfNeeded(f);
      } else if (isL1CacheEnabled && !ctx.isOriginLocal() && !ctx.getLockedKeys().isEmpty()) {
         // We fall into this block if we are a remote node, happen to be the primary data owner and have locked keys.
         // it is still our responsibility to invalidate L1 caches in the cluster.
         blockOnL1FutureIfNeeded(flushL1Caches(ctx));
      }
      return retVal;
   }

   @Override
   protected void prepareOnAffectedNodes(TxInvocationContext ctx, PrepareCommand command, Collection<Address> recipients, boolean sync) {
      Map<Address, Response> responses = rpcManager.invokeRemotely(recipients, command, true, true, false);
      log.debugf("prepare command for transaction %s is sent. responses are: %s",
                 command.getGlobalTransaction().globalId(), responses.toString());

      joinAndSetTransactionVersion(responses.values(), ctx, versionGenerator);
   }

   @Override
   protected InternalCacheEntry retrieveFromRemoteSource(Object key, InvocationContext ctx, boolean acquireRemoteLock, FlagAffectedCommand command)
         throws Exception {
      if (isL1CacheEnabled && ctx instanceof TxInvocationContext) {
         if (log.isTraceEnabled()) {
            log.tracef("Trying to retrieve a the key %s from L1 GMU Data Container", key);
         }
         TxInvocationContext txInvocationContext = (TxInvocationContext) ctx;
         InternalGMUCacheEntry gmuCacheEntry = l1GMUContainer.getValidVersion(key,
                                                                              txInvocationContext.getTransactionVersion(),
                                                                              txInvocationContext.getAlreadyReadFrom());
         if (gmuCacheEntry != null) {
            if (log.isTraceEnabled()) {
               log.tracef("Retrieve a L1 entry for key %s: %s", key, gmuCacheEntry);
            }
            txInvocationContext.addKeyReadInCommand(key, gmuCacheEntry);
            txInvocationContext.addReadFrom(dm.getPrimaryLocation(key));
            return gmuCacheEntry.getInternalCacheEntry();
         }
      }
      if (log.isTraceEnabled()) {
         log.tracef("Failed to retrieve  a L1 entry for key %s", key);
      }
      return performRemoteGet(key, ctx, acquireRemoteLock, command);
   }

   @Override
   protected void storeInL1(Object key, InternalCacheEntry ice, InvocationContext ctx, boolean isWrite, FlagAffectedCommand command) throws Throwable {
      if (log.isTraceEnabled()) {
         log.tracef("Doing a put in L1 into the L1 GMU Data Container");
      }
      InternalGMUCacheEntry gmuCacheEntry = ctx.getKeysReadInCommand().get(key);
      if (gmuCacheEntry == null) {
         throw new NullPointerException("GMU cache entry cannot be null");
      }
      l1GMUContainer.insertOrUpdate(key, gmuCacheEntry);
      CacheEntry ce = ctx.lookupEntry(key);
      if (ce == null || ce.isNull() || ce.isLockPlaceholder() || ce.getValue() == null) {
         if (ce != null && ce.isChanged()) {
            ce.setValue(ice.getValue());
         } else {
            if (isWrite)
               entryFactory.wrapEntryForPut(ctx, key, ice, false, command);
            else
               ctx.putLookedUpEntry(key, ice);
         }
      }
   }

   protected boolean needsRemoteGet(InvocationContext ctx, AbstractDataCommand command) {
      if (command.hasFlag(Flag.CACHE_MODE_LOCAL)
            || command.hasFlag(Flag.SKIP_REMOTE_LOOKUP)
            || command.hasFlag(Flag.IGNORE_RETURN_VALUES)) {
         return false;
      }
      boolean shouldFetchFromRemote = false;
      CacheEntry entry = ctx.lookupEntry(command.getKey());
      if (entry == null || entry.isNull() || entry.isLockPlaceholder()) {
         Object key = command.getKey();
         ConsistentHash ch = stateTransferManager.getCacheTopology().getReadConsistentHash();
         shouldFetchFromRemote = ctx.isOriginLocal() && !ch.isKeyLocalToNode(rpcManager.getAddress(), key) &&
               (!dataContainer.containsKey(key, null) || cacheConfiguration.locking().isolationLevel() == IsolationLevel.SERIALIZABLE);


         InternalGMUCacheEntry igmuce = ctx.getKeysReadInCommand().get(command.getKey());
         boolean unsafeToRead = igmuce != null && igmuce.isUnsafeToRead();
         shouldFetchFromRemote = shouldFetchFromRemote || (ctx.isOriginLocal() && unsafeToRead);
         if (!shouldFetchFromRemote && getLog().isTraceEnabled()) {
            getLog().tracef("Not doing a remote get for key %s since entry is mapped to current node (%s) or is in L1. Owners are %s", key, rpcManager.getAddress(), ch.locateOwners(key));
         }
      }
      return shouldFetchFromRemote;
   }

   protected Object remoteGetAndStoreInL1(InvocationContext ctx, Object key, boolean isWrite, FlagAffectedCommand command) throws Throwable {
      // todo [anistor] fix locality checks in StateTransferManager (ISPN-2401) and use them here
      DataLocality locality = dm.getReadConsistentHash().isKeyLocalToNode(rpcManager.getAddress(), key) ? DataLocality.LOCAL : DataLocality.NOT_LOCAL;

      boolean goInRemote = ctx.isOriginLocal() && !locality.isLocal() && isNotInL1(key) || dm.isAffectedByRehash(key) && !dataContainer.containsKey(key, null);

      InternalGMUCacheEntry entry = ctx.getKeysReadInCommand().get(key);
      boolean unsafeToRead = entry != null && entry.isUnsafeToRead();
      goInRemote = goInRemote || (ctx.isOriginLocal() && unsafeToRead);
      if (goInRemote) {
         if (trace) log.tracef("Doing a remote get for key %s", key);

         boolean acquireRemoteLock = false;
         if (ctx.isInTxScope()) {
            TxInvocationContext txContext = (TxInvocationContext) ctx;
            acquireRemoteLock = isWrite && isPessimisticCache && !txContext.getAffectedKeys().contains(key);
         }
         // attempt a remote lookup
         InternalCacheEntry ice = retrieveFromRemoteSource(key, ctx, acquireRemoteLock, command);

         if (acquireRemoteLock) {
            ((TxInvocationContext) ctx).addAffectedKey(key);
         }

         if (ice != null) {
            if (useClusteredWriteSkewCheck && ctx.isInTxScope()) {
               ((TxInvocationContext) ctx).getCacheTransaction().putLookedUpRemoteVersion(key, ice.getVersion());
            }

            if (isL1CacheEnabled) {
               // We've requested the key only from the owners current (read) CH.
               // If the intersection of owners in the current and pending CHs is empty,
               // the requestor information might be lost, so we shouldn't store the entry in L1.
               // TODO We don't have access to the pending CH here, so we just check if the owners list changed.
               List<Address> readOwners = dm.getReadConsistentHash().locateOwners(key);
               List<Address> writeOwners = dm.getWriteConsistentHash().locateOwners(key);
               if (!readOwners.equals(writeOwners)) {
                  // todo [anistor] this check is not optimal and can yield false positives. here we should use StateTransferManager.isStateTransferInProgressForKey(key) after ISPN-2401 is fixed
                  if (trace) log.tracef("State transfer in progress for key %s, not storing to L1");
                  return ice.getValue();
               }


               if (trace) log.tracef("Caching remotely retrieved entry for key %s in L1", key);
               // This should be fail-safe
               storeInL1(key, ice, ctx, isWrite, command);
            } else {
               if (!ctx.replaceValue(key, ice)) {
                  if (isWrite)
                     lockAndWrap(ctx, key, ice, command);
                  else
                     ctx.putLookedUpEntry(key, ice);
               }
            }
            return ice.getValue();
         }
      } else {
         if (trace)
            log.tracef("Not doing a remote get for key %s since entry is mapped to current node (%s), or is in L1.  Owners are %s", key, rpcManager.getAddress(), dm.locate(key));
      }
      return null;
   }

   @Override
   protected Object localGet(InvocationContext ctx, Object key, boolean isWrite, FlagAffectedCommand command) throws Throwable {
      //this code is performed after the remote get (if any) return nulls.
      return null;
   }

   private InternalCacheEntry performRemoteGet(Object key, InvocationContext ctx, boolean acquireRemoteLock, FlagAffectedCommand command) throws Exception {
      if (ctx instanceof SingleKeyNonTxInvocationContext) {
         return retrieveSingleKeyFromRemoteSource(key, (SingleKeyNonTxInvocationContext) ctx, command);
      } else if (ctx instanceof TxInvocationContext) {
         return retrieveTransactionalGetFromRemoteSource(key, (TxInvocationContext) ctx, acquireRemoteLock, command);
      }
      throw new IllegalStateException("Only handles transaction context or single key gets");
   }

   private InternalCacheEntry retrieveTransactionalGetFromRemoteSource(Object key, TxInvocationContext txInvocationContext,
                                                                       boolean acquireRemoteLock, FlagAffectedCommand command) {
      GlobalTransaction gtx = acquireRemoteLock ? txInvocationContext.getGlobalTransaction() : null;

      ConsistentHash pendingCH = stateTransferManager.getCacheTopology().getPendingCH();

      List<Address> allTargets = new ArrayList<Address>(stateTransferManager.getCacheTopology().getWriteConsistentHash().locateOwners(key));
      allTargets.retainAll(rpcManager.getTransport().getMembers());
      List<Address> oldTargets = null;
      if (pendingCH != null) { //We are executing this get during a rebalance
         oldTargets = new ArrayList<Address>(stateTransferManager.getCacheTopology().getReadConsistentHash().locateOwners(key));
         oldTargets.retainAll(rpcManager.getTransport().getMembers());
      }

      final InternalGMUCacheEntry entry = txInvocationContext.getKeysReadInCommand().get(key);
      boolean unsafeToRead = entry != null && entry.isUnsafeToRead();
      if (unsafeToRead) {
         if (oldTargets == null) {
            oldTargets = new ArrayList<Address>();
         }
         oldTargets.addAll(stateConsumer.oldOwners(key));
      }

      final Collection<Address> alreadyReadFrom = txInvocationContext.getAlreadyReadFrom();
      final GMUVersion transactionVersion = (GMUVersion) txInvocationContext.getTransactionVersion();

      final BitSet alreadyReadFromMask = toAlreadyReadFromMask(alreadyReadFrom, versionGenerator,
                                                               transactionVersion.getViewId());

      final ClusteredGetCommand get = cf.buildGMUClusteredGetCommand(key, command.getFlags(), acquireRemoteLock,
                                                                     gtx, transactionVersion, alreadyReadFromMask);

      final ClusterGetResponse response = doRemote(allTargets, oldTargets, get);
      final InternalGMUCacheValue gmuCacheValue = (InternalGMUCacheValue) response.value;

      final InternalGMUCacheEntry gmuCacheEntry = (InternalGMUCacheEntry) gmuCacheValue.toInternalCacheEntry(key);
      txInvocationContext.addKeyReadInCommand(key, gmuCacheEntry);
      txInvocationContext.addReadFrom(response.sender);

      if (log.isDebugEnabled()) {
         log.debugf("Remote Get successful for transaction %s and key %s. Return value is %s",
                    txInvocationContext.getGlobalTransaction().globalId(), key, gmuCacheValue);
      }
      return gmuCacheEntry;
   }

   private InternalCacheEntry retrieveSingleKeyFromRemoteSource(Object key, SingleKeyNonTxInvocationContext ctx, FlagAffectedCommand command) {
      ConsistentHash pendingCH = stateTransferManager.getCacheTopology().getPendingCH();

      List<Address> allTargets = new ArrayList<Address>(stateTransferManager.getCacheTopology().getWriteConsistentHash().locateOwners(key));
      allTargets.retainAll(rpcManager.getTransport().getMembers());
      List<Address> oldTargets = null;
      if (pendingCH != null) { //We are executing this get during a rebalance
         oldTargets = new ArrayList<Address>(stateTransferManager.getCacheTopology().getReadConsistentHash().locateOwners(key));
         oldTargets.retainAll(rpcManager.getTransport().getMembers());
      }

      final ClusteredGetCommand get = cf.buildGMUClusteredGetCommand(key, command.getFlags(), false, null,
                                                                     commitLog.getCurrentVersion(), null);

      final ClusterGetResponse response = doRemote(allTargets, oldTargets, get);
      final InternalGMUCacheValue gmuCacheValue = (InternalGMUCacheValue) response.value;
      final InternalGMUCacheEntry gmuCacheEntry = (InternalGMUCacheEntry) gmuCacheValue.toInternalCacheEntry(key);
      ctx.addKeyReadInCommand(key, gmuCacheEntry);

      if (log.isDebugEnabled()) {
         log.debugf("Remote Get successful for single key %s. Return value is %s", key, gmuCacheValue);
      }
      return gmuCacheEntry;
   }

   @SuppressWarnings("unchecked")
   private ClusterGetResponse doRemote(List<Address> allTargets, List<Address> oldTargets, ClusteredGetCommand command) {
      final Address localAddress = rpcManager.getAddress();
      final long spinTimeout = 1000;
      final Set<Address> alreadyTargeted = new HashSet<Address>();
      final Object key = command.getKey();

      /*
      * We try to contact the new targets before. Then the old targets. This is for two main reasons:
      * 1) If there is a state transfer in progress we try to contact before the new nodes that will have fresher versions.
      *    If that is not the case we contact the old nodes.
      * 2) If the state transfer is just terminated we try to contact the new targets. All these nodes can send the address of
      *     old donors since they didn't finalize yet the state transfer OR the requestor wants a "very old" version.
      */

      List<Address> newTargets = new ArrayList<Address>(allTargets);
      if (oldTargets != null) {
         newTargets.removeAll(oldTargets);
      } else {
         oldTargets = new ArrayList<Address>();
      }

      while (!newTargets.isEmpty()) {
         Address target = newTargets.remove(0);
         alreadyTargeted.add(target);
         try {
            Object result = null;
            boolean local = target.equals(localAddress);
            if (log.isDebugEnabled()) {
               log.debugf("Perform remote get for key [%s]. Command=%s, target=%s, local?=%s",
                          key, command, target, local);
            }
            if (local) {
               cf.initializeReplicableCommand(command, true);
               result = command.perform(null);
            } else {
               Collection<Address> reallyTargets = Collections.singleton(target);
               ResponseFilter filter = new ClusteredGetResponseValidityFilter(reallyTargets, localAddress);
               Map<Address, Response> responses = rpcManager.invokeRemotely(reallyTargets, command, ResponseMode.WAIT_FOR_VALID_RESPONSE,
                                                                            spinTimeout, true, filter, false);
               Response r = responses.get(target);
               if (r == null) {
                  continue;
               }
               if (r instanceof SuccessfulResponse) {
                  result = ((SuccessfulResponse) r).getResponseValue();
               }
            }
            if (log.isDebugEnabled()) {
               log.debugf("Remote get done for key [%s]. Target=%s, local?=%s, response=%s",
                          key, target, local, result);
            }
            if (result instanceof InternalCacheValue) {
               return new ClusterGetResponse(target, (InternalCacheValue) result);
            } else if (result instanceof Collection) {
               Collection<Address> oldOwners = (Collection<Address>) result;
               for (Address oldOwner : oldOwners) {
                  if (!rpcManager.getMembers().contains(oldOwner)) {
                     alreadyTargeted.add(oldOwner); //owner already left the cluster
                  } else if (!alreadyTargeted.contains(oldOwner)) {
                     oldTargets.add(oldOwner);
                  }
               }
            }
         } catch (Throwable throwable) {
            //no-op
         }
      }


      while (!oldTargets.isEmpty()) {
         Address target = oldTargets.remove(0);
         alreadyTargeted.add(target);
         try {
            Object result = null;
            boolean local = target.equals(localAddress);
            if (log.isDebugEnabled()) {
               log.debugf("Perform remote get for key [%s]. Command=%s, target=%s, local?=%s",
                          key, command, target, local);
            }
            if (local) {
               continue;
            } else {
               Collection<Address> reallyTargets = Collections.singleton(target);
               ResponseFilter filter = new ClusteredGetResponseValidityFilter(reallyTargets, localAddress);
               Map<Address, Response> responses = rpcManager.invokeRemotely(reallyTargets, command, ResponseMode.WAIT_FOR_VALID_RESPONSE,
                                                                            spinTimeout, true, filter, false);
               Response r = responses.get(target);
               if (r == null) {
                  continue;
               }
               if (r instanceof SuccessfulResponse) {
                  result = ((SuccessfulResponse) r).getResponseValue();
               }
            }
            if (log.isDebugEnabled()) {
               log.debugf("Remote get done for key [%s]. Target=%s, local?=%s, response=%s",
                          key, target, local, result);
            }
            if (result instanceof InternalCacheValue) {
               return new ClusterGetResponse(target, (InternalCacheValue) result);
            }
         } catch (Throwable throwable) {
            //no-op
         }
      }

      throw new VersionNotAvailableException(command.getKey());
   }

   private class ClusterGetResponse {
      private final Address sender;
      private final InternalCacheValue value;

      private ClusterGetResponse(Address sender, InternalCacheValue value) {
         this.sender = sender;
         this.value = value;
      }
   }

}
