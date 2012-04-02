package org.infinispan.stats;

/**
 * Websiste: www.cloudtm.eu
 * Date: 01/05/12
 * @author Diego Didona <didona@gsd.inesc-id.pt>
 * @since 5.2
 */
public interface StatisticsContainer {

   void addValue(int param, double value);
   long getValue(int param);
   void mergeTo(StatisticsContainer sc);
   int size();
   void dump();

}
