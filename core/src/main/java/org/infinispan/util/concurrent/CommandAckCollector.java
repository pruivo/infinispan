package org.infinispan.util.concurrent;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commons.util.Util;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public class CommandAckCollector {

   private static final Log log = LogFactory.getLog(CommandAckCollector.class);
   private static final boolean trace = log.isTraceEnabled();

   private final ConcurrentHashMap<CommandInvocationId, CollectorImpl> collectorMap;

   public CommandAckCollector() {
      collectorMap = new ConcurrentHashMap<>();
   }

   public void getOrCreate(CommandInvocationId id) {
      collectorMap.computeIfAbsent(id, commandInvocationId -> {
         CollectorImpl collector = new CollectorImpl();
         collector.completableFuture.thenRun(() -> collectorMap.remove(commandInvocationId));
         if (trace) {
            log.tracef("[Collector#%s] Created new collector for %s.", collector.hashCode(), id);
         }
         return collector;
      });
   }

   public void ack(CommandInvocationId id, Address from, Object previousValue) {
      CollectorImpl collector = collectorMap.get(id);
      if (collector != null) {
         collector.ack(from, previousValue);
      }
   }

   public Object awaitCollector(CommandInvocationId id, long timeout, TimeUnit timeUnit, Object returnValue) throws InterruptedException {
      CollectorImpl collector = collectorMap.get(id);
      if (collector != null) {
         return collector.await(timeout, timeUnit);
      }
      return returnValue;
   }

   private static class CollectorImpl {
      private final CompletableFuture<Object> completableFuture;

      private CollectorImpl() {
         this.completableFuture = new CompletableFuture<>();
      }

      public void ack(Address from, Object previousValue) {
         completableFuture.complete(previousValue);
         if (trace) {
            log.tracef("[Collector#%s]Ack received from %s.", (Object) hashCode(), from);
         }
      }

      public Object await(long timeout, TimeUnit timeUnit) throws InterruptedException {
         try {
            return completableFuture.get(timeout, timeUnit);
         } catch (ExecutionException e) {
            throw new IllegalStateException("this should never happen");
         } catch (java.util.concurrent.TimeoutException e) {
            throw log.timeoutWaitingForAcks(Util.prettyPrintTime(timeout, timeUnit));
         }
      }
   }

}
