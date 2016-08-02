package org.infinispan.remoting.transport.jgroups;

import org.infinispan.remoting.responses.Response;
import org.infinispan.util.concurrent.TimeoutException;
import org.jgroups.blocks.GroupRequest;
import org.jgroups.util.RspList;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * @author Dan Berindei
 * @since 8.0
 */
public class RspListFuture extends CompletableFuture<RspList<Response>> implements Callable<Void> {
   private final GroupRequest<Response> request;
   private volatile Future<?> timeoutFuture = null;

   RspListFuture(GroupRequest<Response> request) {
      this.request = request;
      request.whenComplete((rsps, throwable) -> {
         if (throwable == null) {
            complete(rsps);
         } else {
            request.
            completeExceptionally(throwable);
         }
      });
      if (timeoutFuture != null) {
         timeoutFuture.cancel(false);
      }
   }

   public void setTimeoutFuture(Future<?> timeoutFuture) {
      this.timeoutFuture = timeoutFuture;
      if (isDone()) {
         timeoutFuture.cancel(false);
      }
   }

   @Override
   public Void call() throws Exception {
      // The request timed out
      completeExceptionally(new TimeoutException("Timed out waiting for responses"));
      request.cancel(true);
      return null;
   }
}
