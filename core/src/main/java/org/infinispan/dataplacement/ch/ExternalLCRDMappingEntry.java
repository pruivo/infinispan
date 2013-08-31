package org.infinispan.dataplacement.ch;

/**
 * @author Pedro Ruivo
 * @since 5.2
 */
public class ExternalLCRDMappingEntry {

   private final String transactionClass;
   private final LCRDCluster[] clusters;

   public ExternalLCRDMappingEntry(String transactionClass, int size) {
      this.transactionClass = transactionClass;
      this.clusters = new LCRDCluster[size];
   }

   public final void set(int index, LCRDCluster cluster) {
      clusters[index] = cluster;
   }

   public String getTransactionClass() {
      return transactionClass;
   }

   public LCRDCluster[] getClusters() {
      return clusters;
   }

   public LCRDCluster getLastCluster() {
      return clusters[clusters.length - 1];
   }
}
