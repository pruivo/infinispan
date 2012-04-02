package org.infinispan.stats.translations;

import java.util.EnumMap;
import java.util.Map;
import static org.infinispan.stats.translations.ExposedStatistics.IspnStats;

/**
 * Websiste: www.cloudtm.eu
 * Date: 01/05/12
 * @author Diego Didona <didona@gsd.inesc-id.pt>
 * @author Pedro Ruivo
 * @since 5.2
 */
public class RemoteStatistics {
   public static final int NOT_FOUND = -1;
   private static final Map<IspnStats, Integer> translationMap = new EnumMap<IspnStats, Integer>(IspnStats.class);

   static {
      int i = 0;
      for (IspnStats stat : IspnStats.values()) {
         if (stat.isRemote()) {
            translationMap.put(stat, i++);
         }
      }
   }

   public static int getIndex(IspnStats stat) {
      Integer idx = translationMap.get(stat);      
      return idx == null ? NOT_FOUND : idx;
   }

   public static int getSize() {
      return translationMap.size();
   }
}
