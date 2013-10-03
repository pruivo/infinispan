/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.statetransfer;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.transaction.gmu.manager.SortedTransactionQueue;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

/**
 * A representation of a transaction that is suitable for transferring between a StateProvider and a StateConsumer
 * running on different members of the same cache.
 *
 * @author anistor@redhat.com
 * @author Sebastiano Peluso
 * @since 5.2
 */
public class TransactionInfo {

   private static final Log log = LogFactory.getLog(TransactionInfo.class);

   protected final GlobalTransaction globalTransaction;

   protected final WriteCommand[] modifications;

   protected final Set<Object> lockedKeys;

   protected final int topologyId;

   protected final boolean isShadow;

   protected final EntryVersion version;

   protected transient volatile SortedTransactionQueue.TransactionEntry transactionEntry;

   protected transient CountDownLatch accessTransactionEntry;

   public TransactionInfo(GlobalTransaction globalTransaction, int topologyId, WriteCommand[] modifications, Set<Object> lockedKeys) {
      this.globalTransaction = globalTransaction;
      this.topologyId = topologyId;
      this.modifications = modifications;
      this.lockedKeys = lockedKeys;
      this.isShadow = false;
      this.version = null;
      this.accessTransactionEntry = new CountDownLatch(1);
      this.transactionEntry = null;
   }

   public TransactionInfo(int topologyId, EntryVersion transferVersion) {
      this.globalTransaction = null;
      this.topologyId = topologyId;
      this.modifications = null;
      this.lockedKeys = null;
      this.isShadow = true;
      this.version = transferVersion;
      this.accessTransactionEntry = new CountDownLatch(1);
      this.transactionEntry = null;
   }

   public GlobalTransaction getGlobalTransaction() {
      return globalTransaction;
   }

   public WriteCommand[] getModifications() {
      return modifications;
   }

   public Set<Object> getLockedKeys() {
      return lockedKeys;
   }

   public int getTopologyId() {
      return topologyId;
   }

   public boolean isShadow() {
      return isShadow;
   }

   public EntryVersion getVersion() {
      return this.version;
   }

   public SortedTransactionQueue.TransactionEntry waitForTransactionEntry() {
      if(this.transactionEntry != null)
         return this.transactionEntry;
      else{
         try {
            this.accessTransactionEntry.await();
         } catch (InterruptedException e) {
            e.printStackTrace();
         }

         return this.transactionEntry;
      }
   }

   public void setTransactionEntry(SortedTransactionQueue.TransactionEntry transactionEntry) {
      this.transactionEntry = transactionEntry;
      this.accessTransactionEntry.countDown();
   }

   @Override
   public String toString() {
      return "TransactionInfo{" +
            "globalTransaction=" + globalTransaction +
            ", topologyId=" + topologyId +
            ", modifications=" + ((modifications==null)?null:Arrays.asList(modifications)) +
            ", lockedKeys=" + lockedKeys +
            ", isShadow=" + isShadow +
            ", version=" + version +
            '}';
   }

   public static class Externalizer extends AbstractExternalizer<TransactionInfo> {

      @Override
      public Integer getId() {
         return Ids.TRANSACTION_INFO;
      }

      @Override
      public Set<Class<? extends TransactionInfo>> getTypeClasses() {
         return Collections.<Class<? extends TransactionInfo>>singleton(TransactionInfo.class);
      }

      @Override
      public void writeObject(ObjectOutput output, TransactionInfo object) throws IOException {
         output.writeBoolean(object.isShadow);
         output.writeInt(object.topologyId);
         if(!object.isShadow){
            output.writeObject(object.globalTransaction);
            output.writeObject(object.modifications);
            output.writeObject(object.lockedKeys);
         }
         else{
            if (object.version == null) {
               output.writeBoolean(false);
            } else {
               output.writeBoolean(true);
               output.writeObject(object.version);
            }
         }
      }

      @Override
      @SuppressWarnings("unchecked")
      public TransactionInfo readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Boolean isShadow = input.readBoolean();
         int topologyId = input.readInt();
         if(!isShadow) {
            GlobalTransaction globalTransaction = (GlobalTransaction) input.readObject();
            WriteCommand[] modifications = (WriteCommand[]) input.readObject();
            Set<Object> lockedKeys = (Set<Object>) input.readObject();
            return new TransactionInfo(globalTransaction, topologyId, modifications, lockedKeys);
         }
         else{
            EntryVersion version = null;
            if(input.readBoolean()){
               version = (EntryVersion) input.readObject();
            }
            return new TransactionInfo(topologyId, version);
         }
      }
   }
}
