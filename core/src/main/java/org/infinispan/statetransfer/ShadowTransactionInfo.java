package org.infinispan.statetransfer;


import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.transaction.gmu.manager.SortedTransactionQueue;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

/**
 * @author Sebastiano Peluso
 */
public class ShadowTransactionInfo extends TransactionInfo {

   private EntryVersion version;
   private transient volatile SortedTransactionQueue.TransactionEntry transactionEntry;

   public ShadowTransactionInfo(int topologyId, EntryVersion donorCurrentVersion) {
      super(null, topologyId, null, null);
      version = donorCurrentVersion;
      transactionEntry = null;
   }

   public EntryVersion getVersion() {
      return this.version;
   }

   public SortedTransactionQueue.TransactionEntry getTransactionEntry() {
      return this.transactionEntry;
   }

   public void setTransactionEntry(SortedTransactionQueue.TransactionEntry transactionEntry) {
      this.transactionEntry = transactionEntry;
   }

   @Override
   public String toString() {
      return "TransactionInfo{" +
            ", topologyId=" + topologyId +
            ", version=" + version +
            '}';
   }

   public static class Externalizer extends AbstractExternalizer<ShadowTransactionInfo> {

      @Override
      public Integer getId() {
         return Ids.SHADOW_TRANSACTION_INFO;
      }

      @Override
      public Set<Class<? extends ShadowTransactionInfo>> getTypeClasses() {
         return Collections.<Class<? extends ShadowTransactionInfo>>singleton(ShadowTransactionInfo.class);
      }

      @Override
      public void writeObject(ObjectOutput output, ShadowTransactionInfo object) throws IOException {
         output.writeInt(object.topologyId);
         output.writeObject(object.version);
      }

      @Override
      @SuppressWarnings("unchecked")
      public ShadowTransactionInfo readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         int topologyId = input.readInt();
         EntryVersion version = (EntryVersion) input.readObject();
         return new ShadowTransactionInfo(topologyId, version);
      }
   }
}
