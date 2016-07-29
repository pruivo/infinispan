package org.infinispan.remoting.transport.jgroups;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.infinispan.remoting.responses.Response;

/**
 * @author Dan Berindei
 * @since 8.0
 */
public class SingleResponseFuture extends CompletableFuture<Response> implements Runnable {
   private final CompletableFuture<Response> request;
   private volatile Future<?> timeoutFuture = null;

   SingleResponseFuture(CompletableFuture<Response> request) {
      this.request = request;
      this.request.whenComplete(this::notify);
   }

   private void notify(Response response, Throwable throwable) {
      if (throwable != null) {
         completeExceptionally(throwable instanceof CompletionException ? throwable.getCause() : throwable);
      } else {
         complete(response);
      }
      cancelTimeoutFuture();
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

   private void cancelTimeoutFuture() {
      Future<?> f = timeoutFuture;
      if (f != null) {
         f.cancel(false);
      }
   }

   //timeout!
   @Override
   public void run() {
      request.cancel(false);
   }
}
