package org.infinispan.dataplacement.stats;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Task that saves the round statistics to a file
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class SaveStatsTask implements Runnable {

   private static final Log log = LogFactory.getLog(SaveStatsTask.class);
   private static final String PATH = "./stats.csv";
   private static final AtomicBoolean FIRST_TIME = new AtomicBoolean(true);
   private final Stats stats;

   public SaveStatsTask(Stats stats) {
      this.stats = stats;
   }

   @Override
   public void run() {
      try {
         log.errorf("Save stats to %s, first time %s", PATH, FIRST_TIME.get());
         BufferedWriter writer = new BufferedWriter(new FileWriter(PATH, !FIRST_TIME.get()));
         stats.saveTo(writer, FIRST_TIME.compareAndSet(true, false));
         writer.flush();
         writer.close();
      } catch (Exception e) {
         log.errorf(e, "Error saving stats to %s", PATH);
      }
   }
}
