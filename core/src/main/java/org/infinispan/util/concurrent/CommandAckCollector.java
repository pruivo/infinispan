package org.infinispan.util.concurrent;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commons.util.Util;
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

   private final ConcurrentHashMap<CommandInvocationId, CompletableFuture<Object>> collectorMap;

   public CommandAckCollector() {
      collectorMap = new ConcurrentHashMap<>();
   }

   public void create(CommandInvocationId id) {
      collectorMap.putIfAbsent(id, new CompletableFuture<>());
      if (trace) {
         log.tracef("Created new collector for %s.", id);
      }
   }

   public void ack(CommandInvocationId id, Object previousValue) {
      CompletableFuture<Object> collector = collectorMap.get(id);
      if (collector != null) {
         collector.complete(previousValue);
      }
   }

   public Object awaitCollector(CommandInvocationId id, long timeout, TimeUnit timeUnit, Object returnValue) throws InterruptedException {
      CompletableFuture<Object> collector = collectorMap.get(id);
      if (collector != null) {
         try {
            return collector.get(timeout, timeUnit);
         } catch (ExecutionException e) {
            throw new IllegalStateException();
         } catch (java.util.concurrent.TimeoutException e) {
            throw log.timeoutWaitingForAcks(Util.prettyPrintTime(timeout, timeUnit));
         } finally {
            collectorMap.remove(id);
         }
      }
      return returnValue;
   }
}
