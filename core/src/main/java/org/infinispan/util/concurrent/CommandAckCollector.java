package org.infinispan.util.concurrent;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commons.util.Util;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
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

   private static final Log log = LogFactory.getLog(Log.class);
   private static final boolean trace = log.isTraceEnabled();

   private final ConcurrentHashMap<CommandInvocationId, CollectorImpl> collectorMap;

   public CommandAckCollector() {
      collectorMap = new ConcurrentHashMap<>();
   }

   public CompletableFuture<Void> getOrCreate(CommandInvocationId id, Collection<Address> expected) {
      if (expected == null || expected.isEmpty()) {
         return completedFuture(id);
      }
      //make a copy!
      Set<Address> uniqueExpected = new HashSet<>(expected);
      return collectorMap.computeIfAbsent(id, commandInvocationId -> {
         CollectorImpl collector = new CollectorImpl(uniqueExpected);
         collector.getFuture().thenRun(() -> collectorMap.remove(commandInvocationId));
         if (trace) {
            log.tracef("[Collector#%s] Created new collector for %s. Expected acks=%s", collector.hashCode(), id, uniqueExpected);
         }
         return collector;
      }).getFuture();
   }

   public void ack(CommandInvocationId id, Address from) {
      CollectorImpl collector = collectorMap.get(id);
      if (collector != null) {
         collector.ack(from);
      }
   }

   public void awaitCollector(CommandInvocationId id, long timeout, TimeUnit timeUnit) throws InterruptedException {
      CollectorImpl collector = collectorMap.get(id);
      if (collector != null) {
         collector.await(timeout, timeUnit);
      }
   }

   public void cancelCollector(CommandInvocationId id) {
      CollectorImpl collector = collectorMap.remove(id);
      if (collector != null) {
         collector.completableFuture.complete(null);
      }
   }

   public CompletableFuture<Void> get(CommandInvocationId id) {
      CollectorImpl collector = collectorMap.get(id);
      return collector == null ? completedFuture(id) : collector.getFuture();
   }

   private CompletableFuture<Void> completedFuture(CommandInvocationId id) {
      if (trace) {
         log.tracef("EMPTY collector for %s", id);
      }
      return CompletableFutures.completedNull();
   }

   private static class CollectorImpl {
      private final Collection<Address> confirmationNeeded;
      private final CompletableFuture<Void> completableFuture;

      private CollectorImpl(Collection<Address> confirmationNeeded) {
         this.confirmationNeeded = confirmationNeeded;
         this.completableFuture = new CompletableFuture<>();
      }

      public void ack(Address from) {
         final boolean isLast = isLastAck(from);
         if (isLast) {
            completableFuture.complete(null);
         }
         if (trace) {
            log.tracef("[Collector#%s]Ack received from %s. Is last ack=%s", (Object) hashCode(), from, isLast);
         }
      }

      public void await(long timeout, TimeUnit timeUnit) throws InterruptedException {
         try {
            completableFuture.get(timeout, timeUnit);
         } catch (ExecutionException e) {
            throw new IllegalStateException("this should never happen");
         } catch (java.util.concurrent.TimeoutException e) {
            throw log.timeoutWaitingForAcks(Util.prettyPrintTime(timeout, timeUnit), String.valueOf(confirmationNeeded));
         }
      }

      public CompletableFuture<Void> getFuture() {
         return completableFuture;
      }

      private synchronized boolean isLastAck(Address from) {
         return confirmationNeeded.remove(from) && confirmationNeeded.isEmpty();
      }
   }

}
