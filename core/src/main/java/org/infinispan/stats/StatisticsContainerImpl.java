package org.infinispan.stats;

/**
 * Websiste: www.cloudtm.eu
 * Date: 01/05/12
 * @author Diego Didona <didona@gsd.inesc-id.pt>
 * @since 5.2
 */
public class StatisticsContainerImpl implements StatisticsContainer{

   private final long[] stats;

   public StatisticsContainerImpl(int size){
      this.stats = new long[size];
   }

   public final void addValue(int param, double value){
      this.stats[param]+=value;
   }

   public final long getValue(int param){
      return this.stats[param];
   }

   public final void mergeTo(StatisticsContainer sc){
      int length = this.stats.length;
      for(int i = 0; i < length; i++){
         sc.addValue(i,this.stats[i]);
      }
   }

   public final int size(){
      return this.stats.length;
   }

   public final void dump(){
      for(int i=0; i<this.stats.length;i++){
         System.out.println("** "+i+" : "+stats[i]+" **");
      }
   }
}
