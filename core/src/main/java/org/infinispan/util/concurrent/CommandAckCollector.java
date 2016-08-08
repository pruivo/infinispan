package org.infinispan.util.concurrent;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commons.util.Util;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * An ack collector for Triangle algorithm used in non-transactional caches.
 * <p>
 * Acks are used between the originator and the owners of a key.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class CommandAckCollector {

   private static final Log log = LogFactory.getLog(CommandAckCollector.class);
   private static final boolean trace = log.isTraceEnabled();

   private final ConcurrentHashMap<CommandInvocationId, Collector> collectorMap;

   public CommandAckCollector() {
      collectorMap = new ConcurrentHashMap<>();
   }

   /**
    * Creates a collector for a write command.
    *
    * @param id      The command invocation id.
    * @param backups The backups to wait for. It must be non-{@code null} and non empty.
    */
   public void create(CommandInvocationId id, Collection<Address> backups) {
      collectorMap.putIfAbsent(id, new Collector(backups));
      if (trace) {
         log.tracef("Created new collector for %s. Backups=%s", id, backups);
      }
   }

   /**
    * Creates a collector for a write command.
    * <p>
    * It differs from {@link #create(CommandInvocationId, Collection)} by having already the return value. It must be
    * used when the primary owner of key is the originator of the command. In that case, it only needs to wait for
    * backups acks.
    *
    * @param id          The command invocation id.
    * @param returnValue The command return value.
    * @param backups     The backups to wait for. It must be non-{@code null} and non empty.
    */
   public void create(CommandInvocationId id, Object returnValue, Collection<Address> backups) {
      collectorMap.putIfAbsent(id, new Collector(returnValue, backups));
      if (trace) {
         log.tracef("Created new collector for %s. ReturnValue=%s. Backups=%s", id, returnValue, backups);
      }
   }

   /**
    * @param id
    * @param from
    */
   public void backupAck(CommandInvocationId id, Address from) {
      Collector collector = collectorMap.get(id);
      if (collector != null) {
         if (trace) {
            log.tracef("[Collector#%s] Backup ACK from %s.", id, from);
         }
         collector.backupAck(from);
      }
   }

   public void primaryAck(CommandInvocationId id, Object returnValue, boolean success) {
      Collector collector = collectorMap.get(id);
      if (collector != null) {
         if (trace) {
            log.tracef("[Collector#%s] Primary ACK. Success=%s. ReturnValue=%s.", id, success, returnValue);
         }
         collector.primaryAck(returnValue, success);
      }
   }

   public Object awaitCollector(CommandInvocationId id, long timeout, TimeUnit timeUnit, Object returnValue) throws InterruptedException {
      Collector collector = collectorMap.get(id);
      if (collector != null) {
         try {
            return collector.get(timeout, timeUnit);
         } finally {
            collectorMap.remove(id);
         }
      }
      return returnValue;
   }

   private static class Collector {
      private final CompletableFuture<Object> future;
      private final Collection<Address> backups;
      private Object primaryReturnValue;
      private boolean primaryAckReceived;

      private Collector(Collection<Address> backups) {
         this.future = new CompletableFuture<>();
         this.backups = backups.isEmpty() ?
               Collections.emptyList() :
               new HashSet<>(backups);
      }

      private Collector(Object returnValue, Collection<Address> backups) {
         this.primaryReturnValue = returnValue;
         this.primaryAckReceived = true;
         if (backups.isEmpty()) {
            this.backups = Collections.emptyList();
            this.future = CompletableFuture.completedFuture(returnValue);
         } else {
            future = new CompletableFuture<>();
            this.backups = new HashSet<>(backups);
         }
      }

      public synchronized void primaryAck(Object returnValue, boolean success) {
         if (primaryAckReceived) {
            return;
         }
         this.primaryReturnValue = returnValue;
         this.primaryAckReceived = true;

         if (!success) {
            //we are not receiving any backups ack!
            backups.clear();
            future.complete(returnValue);
            if (trace) {
               log.tracef("Collector ready. Command not succeed on primary.");
            }
         } else if (backups.isEmpty()) {
            if (trace) {
               log.tracef("Collector ready. No pending backup acks.");
            }
            future.complete(primaryReturnValue);
         }
      }

      public synchronized void backupAck(Address from) {
         if (backups.remove(from) && primaryAckReceived && backups.isEmpty()) {
            if (trace) {
               log.tracef("Collector ready. No pending backup/primary acks.");
            }
            future.complete(primaryReturnValue);
         }
      }

      public Object get(long timeout, TimeUnit timeUnit) throws InterruptedException {
         try {
            return future.get(timeout, timeUnit);
         } catch (ExecutionException e) {
            throw new IllegalStateException();
         } catch (java.util.concurrent.TimeoutException e) {
            throw log.timeoutWaitingForAcks(Util.prettyPrintTime(timeout, timeUnit));
         }
      }
   }
}
