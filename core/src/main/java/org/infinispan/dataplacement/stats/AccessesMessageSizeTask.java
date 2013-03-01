package org.infinispan.dataplacement.stats;

import org.infinispan.dataplacement.AccessesManager;
import org.infinispan.dataplacement.ObjectRequest;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

/**
 * Task that sums the size of all object requests
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class AccessesMessageSizeTask implements Runnable {

   private static final Log log = LogFactory.getLog(AccessesMessageSizeTask.class);
   private final AccessesManager accessesManager;
   private final Stats stats;

   public AccessesMessageSizeTask(Stats stats, AccessesManager accessesManager) {
      this.accessesManager = accessesManager;
      this.stats = stats;
   }

   @Override
   public void run() {
      try {
         int size = 0;
         ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
         ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);

         for (ObjectRequest objectRequest : accessesManager.getAccesses()) {
            objectOutputStream.writeObject(objectRequest);
            objectOutputStream.flush();

            size += byteArrayOutputStream.toByteArray().length;

            byteArrayOutputStream.reset();
         }

         stats.accessesSize(size);
         objectOutputStream.close();
      } catch (IOException e) {
         log.warn("Error calculating object requests size", e);
      }
   }
}
