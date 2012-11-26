/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
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
package org.infinispan.commands.remote;

import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.container.versioning.gmu.GMUVersion;
import org.infinispan.container.versioning.gmu.GMUVersionGenerator;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.gmu.ClusterSnapshot;
import org.infinispan.transaction.gmu.CommitLog;
import org.infinispan.transaction.gmu.VersionNotAvailableException;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.Equivalence;

import java.util.BitSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static org.infinispan.transaction.gmu.GMUHelper.toGMUVersionGenerator;

/**
 * Issues a remote get call.  This is not a {@link org.infinispan.commands.VisitableCommand} and hence not passed up the
 * {@link org.infinispan.interceptors.base.CommandInterceptor} chain.
 *
 * @author Pedro Ruivo
 * @author Sebastiano Peluso
 * @since 5.2
 */
public class GMUClusteredGetCommand extends ClusteredGetCommand {

   public static final byte COMMAND_ID = 42;
   //the transaction version. from this version and with the bit set, it calculates the max and min version to read
   private GMUVersion transactionVersion;
   private BitSet alreadyReadFrom;
   private CommitLog commitLog;
   private GMUVersionGenerator versionGenerator;
   //built in the remote server
   private InvocationContext invocationContext;
   private GetKeyValueCommand command;
   private GMUVersion minGMUVersion;

   public GMUClusteredGetCommand(String cacheName) {
      super(cacheName);
   }

   public GMUClusteredGetCommand(Object key, String cacheName, Set<Flag> flags, boolean acquireRemoteLock,
                                 GlobalTransaction globalTransaction, Equivalence keyEquivalence, GMUVersion txVersion,
                                 BitSet alreadyReadFrom) {
      super(key, cacheName, flags, acquireRemoteLock, globalTransaction, keyEquivalence);
      this.transactionVersion = txVersion;
      this.alreadyReadFrom = alreadyReadFrom == null || alreadyReadFrom.isEmpty() ? null : alreadyReadFrom;
   }

   public GMUClusteredGetCommand() {
      super(null); //for ID uniqueness test
   }

   public void initializeGMUComponents(CommitLog commitLog, VersionGenerator versionGenerator) {
      this.commitLog = commitLog;
      this.versionGenerator = toGMUVersionGenerator(versionGenerator);
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      Object[] original = super.getParameters();
      Object[] retVal = new Object[original.length + 3];
      System.arraycopy(original, 0, retVal, 0, original.length);
      int index = original.length;
      retVal[index++] = transactionVersion;
      retVal[index] = alreadyReadFrom;
      return retVal;
   }

   @Override
   public void setParameters(int commandId, Object[] args) {
      int index = args.length - 3;
      super.setParameters(commandId, args);
      transactionVersion = (GMUVersion) args[index++];
      alreadyReadFrom = (BitSet) args[index];
   }

   @Override
   public String toString() {
      return "GMUClusteredGetCommand{key=" + getKey() +
            ", flags=" + getFlags() +
            ", transactionVersion=" + transactionVersion +
            ", alreadyReadFrom=" + alreadyReadFrom + "}";
   }

   public final void init() {
      command = super.constructCommand(getFlags());
      invocationContext = super.createInvocationContext(command);

      GMUVersion maxGMUVersion;

      boolean alreadyReadOnThisNode;
      if (alreadyReadFrom != null) {
         int txViewId = transactionVersion.getViewId();
         ClusterSnapshot clusterSnapshot = versionGenerator.getClusterSnapshot(txViewId);
         List<Address> addressList = new LinkedList<Address>();
         for (int i = 0; i < clusterSnapshot.size(); ++i) {
            if (alreadyReadFrom.get(i)) {
               addressList.add(clusterSnapshot.get(i));
            }
         }
         minGMUVersion = versionGenerator.calculateMinVersionToRead(transactionVersion, addressList);
         maxGMUVersion = versionGenerator.calculateMaxVersionToRead(transactionVersion, addressList);
         int myIndex = clusterSnapshot.indexOf(versionGenerator.getAddress());
         //to be safe, is better to wait...
         alreadyReadOnThisNode = myIndex != -1 && alreadyReadFrom.get(myIndex);

      } else {
         minGMUVersion = transactionVersion;
         maxGMUVersion = null;
         alreadyReadOnThisNode = false;
      }

      invocationContext.setAlreadyReadOnThisNode(alreadyReadOnThisNode);
      invocationContext.setVersionToRead(commitLog.getAvailableVersionLessThan(maxGMUVersion));

      if (minGMUVersion == null) {
         throw new NullPointerException("Min Version cannot be null");
      }
   }

   public final boolean isReady() {
      return invocationContext.hasAlreadyReadOnThisNode() || commitLog.isMinVersionAvailable(minGMUVersion);
   }

   @Override
   protected InvocationContext createInvocationContext(GetKeyValueCommand command) {
      return invocationContext;
   }

   @Override
   protected GetKeyValueCommand constructCommand(Set<Flag> flags) {
      return command;
   }

   @Override
   protected InternalCacheValue invoke(GetKeyValueCommand command, InvocationContext context) {
      super.invoke(command, context);
      InternalCacheEntry gmuCacheEntry = context.getKeysReadInCommand().get(getKey());
      if (gmuCacheEntry == null) {
         throw new VersionNotAvailableException();
      }
      return gmuCacheEntry.toInternalCacheValue();
   }
}
