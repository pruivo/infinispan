package org.infinispan.remoting.transport.jgroups;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.infinispan.remoting.responses.Response;
import org.jgroups.Address;
import org.jgroups.blocks.GroupRequest;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;

/**
 * @author Dan Berindei
 * @since 8.0
 */
public class RspListFuture extends CompletableFuture<RspList<Response>> implements Runnable {
   private final GroupRequest<Response> request;
   private final Collection<Address> destinations;
   private volatile Future<?> timeoutFuture = null;


   RspListFuture(Collection<Address> destinations, GroupRequest<Response> request) {
      this.destinations = destinations;
      this.request = request;
      this.request.whenComplete(this::handleResponse);
   }

   public void setTimeoutTask(ScheduledExecutorService timeoutExecutor, long timeoutMillis) {
      if (!isDone()) {
         Future<?> f = timeoutExecutor.schedule(this, timeoutMillis, TimeUnit.MILLISECONDS);
         timeoutFuture = f;
         if (isDone()) {
            f.cancel(false);
         }
      }
   }

   private void handleResponse(RspList<Response> rspList, Throwable throwable) {
      if (throwable != null) {
         completeExceptionally(throwable);
      }
      if (rspList.isEmpty() && destinations != null) {
         RspList<Response> newRspList = new RspList<>();
         for (Address address : destinations) {
            Rsp<Response> rsp = new Rsp<>();
            rsp.setSuspected();
            newRspList.put(address, rsp);
         }
         complete(newRspList);
      } else {
         complete(rspList);
      }
      cancelTimeoutFuture();
   }

   private void cancelTimeoutFuture() {
      Future<?> f = timeoutFuture;
      if (f != null) {
         f.cancel(false);
      }
   }

   @Override
   public void run() {
      this.request.cancel(false);
   }
}
