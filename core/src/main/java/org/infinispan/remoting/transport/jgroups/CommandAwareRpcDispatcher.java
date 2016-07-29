package org.infinispan.remoting.transport.jgroups;

import static org.infinispan.remoting.transport.jgroups.JGroupsTransport.fromJGroupsAddress;
import static org.jgroups.Message.Flag.NO_FC;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.infinispan.IllegalLifecycleStateException;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.context.Flag;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.inboundhandler.InboundInvocationHandler;
import org.infinispan.remoting.inboundhandler.Reply;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.util.TimeService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.XSiteReplicateCommand;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.blocks.GroupRequest;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.blocks.RspFilter;
import org.jgroups.protocols.relay.SiteAddress;
import org.jgroups.util.Buffer;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;

/**
 * A JGroups RPC dispatcher that knows how to deal with {@link ReplicableCommand}s.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Pedro Ruivo
 * @since 4.0
 */
public class CommandAwareRpcDispatcher extends RpcDispatcher {
   private static final RspList<Response> EMPTY_RESPONSES_LIST = new RspList<>();

   private static final Log log = LogFactory.getLog(CommandAwareRpcDispatcher.class);
   private static final boolean trace = log.isTraceEnabled();
   private static final boolean FORCE_MCAST = SecurityActions.getBooleanProperty("infinispan.unsafe.force_multicast");
   private static final long STAGGER_DELAY_NANOS = TimeUnit.MILLISECONDS.toNanos(
         SecurityActions.getIntProperty("infinispan.stagger.delay", 5));
   private static final int REPLY_FLAGS_TO_CLEAR = Message.Flag.RSVP.value() |
         Message.Flag.INTERNAL.value() | Message.Flag.NO_TOTAL_ORDER.value();
   private static final int REPLY_FLAGS_TO_SET = NO_FC.value();

   private final InboundInvocationHandler handler;
   private final ScheduledExecutorService timeoutExecutor;
   private final TimeService timeService;
   private final StreamingMarshaller streamMarshalling;

   public CommandAwareRpcDispatcher(JChannel channel, JGroupsTransport transport,
                                    InboundInvocationHandler globalHandler, ScheduledExecutorService timeoutExecutor,
                                    TimeService timeService, StreamingMarshaller streamMarshalling) {
      this.timeService = timeService;
      this.streamMarshalling = streamMarshalling;
      this.server_obj = transport;
      this.handler = globalHandler;
      this.timeoutExecutor = timeoutExecutor;

      // MessageDispatcher superclass constructors will call start() so perform all init here
      this.setMembershipListener(transport);
      this.setChannel(channel);
      // If existing up handler is a muxing up handler, setChannel(..) will not have replaced it
      /*UpHandler handler = channel.getUpHandler();
      if (handler instanceof Muxer<?>) {
         @SuppressWarnings("unchecked")
         Muxer<UpHandler> mux = (Muxer<UpHandler>) handler;
         mux.setDefaultHandler(this.prot_adapter);
      }*/
      channel.addChannelListener(this);
      asyncDispatching(true);
   }

   @Override
   public void close() {
      // Ensure dispatcher is stopped
      this.stop();
      // We must unregister our listener, otherwise the channel will retain a reference to this dispatcher
      this.channel.removeChannelListener(this);
   }

   /*@Override
   protected RequestCorrelator createRequestCorrelator(Protocol transport, RequestHandler handler, Address local_addr) {
      return new CustomRequestCorrelator(transport, handler, local_addr);
   }*/

   private boolean isValid(Message req) {
      if (req == null || req.getLength() == 0) {
         log.msgOrMsgBufferEmpty();
         return false;
      }

      return true;
   }

   /**
    * @param recipients Must <b>not</b> contain self.
    */
   public CompletableFuture<RspList<Response>> invokeRemoteCommands(List<Address> recipients, ReplicableCommand command,
                                                                    ResponseMode mode, long timeout, RspFilter filter,
                                                                    DeliverOrder deliverOrder) {
      CompletableFuture<RspList<Response>> future;
      try {
         if (recipients != null && mode == ResponseMode.GET_FIRST && STAGGER_DELAY_NANOS > 0) {
            future = new CompletableFuture<>();
            // We populate the RspList ahead of time to avoid additional synchronization afterwards
            RspList<Response> rsps = new RspList<>();
            for (Address recipient : recipients) {
               rsps.put(recipient, new Rsp<>());
            }
            // This isn't really documented, but some of our internal code uses timeout = 0 as no timeout.
            long nanoTimeout = timeout > 0 ? TimeUnit.MILLISECONDS.toNanos(timeout) : Long.MAX_VALUE;
            long deadline = timeService.expectedEndTime(nanoTimeout, TimeUnit.NANOSECONDS);
            processCallsStaggered(command, filter, recipients, mode, deliverOrder, streamMarshalling, future,
                  0, deadline, rsps);
         } else {
            future = processCalls(command, recipients == null, timeout, filter, recipients, mode, deliverOrder,
                    streamMarshalling);
         }
         return future;
      } catch (Exception e) {
         return rethrowAsCacheException(e);
      }
   }

   public SingleResponseFuture invokeRemoteCommand(Address recipient, ReplicableCommand command,
                                                   ResponseMode mode, long timeout,
                                                   DeliverOrder deliverOrder) {
      SingleResponseFuture future;
      try {
         future = processSingleCall(command, timeout, recipient, mode, deliverOrder, streamMarshalling);
         return future;
      } catch (Exception e) {
         return rethrowAsCacheException(e);
      }
   }

   public <T> T rethrowAsCacheException(Throwable t) {
      if (t instanceof CacheException)
         throw (CacheException) t;
      else
         throw new CacheException(t);
   }

   /**
    * Message contains a Command. Execute it against *this* object and return result.
    */
   @Override
   public void handle(Message req, org.jgroups.blocks.Response response) throws Exception {
      if (isValid(req)) {
         ReplicableCommand cmd = null;
         try {
            cmd = (ReplicableCommand) streamMarshalling.objectFromByteBuffer(req.getRawBuffer(), req.getOffset(), req.getLength());
            if (cmd == null)
               throw new NullPointerException("Unable to execute a null command!  Message was " + req);
            if (req.getSrc() instanceof SiteAddress) {
               executeCommandFromRemoteSite(cmd, req, response);
            } else {
               executeCommandFromLocalCluster(cmd, req, response);
            }
         } catch (InterruptedException e) {
            log.shutdownHandlingCommand(cmd);
            reply(response, new ExceptionResponse(new CacheException("Cache is shutting down")), cmd, req);
         } catch (IllegalLifecycleStateException e) {
            if (trace) log.trace("Ignoring command unmarshalling error during shutdown");
            // If this wasn't a CacheRpcCommand, it means the channel is already stopped, and the response won't matter
            reply(response, CacheNotFoundResponse.INSTANCE, cmd, req);
         } catch (Throwable x) {
            if (cmd == null)
               log.errorUnMarshallingCommand(x);
            else
               log.exceptionHandlingCommand(cmd, x);
            reply(response, new ExceptionResponse(new CacheException("Problems invoking command.", x)), cmd,
                  req);
         }
      } else {
         reply(response, null, null, req);
      }
   }

   private void executeCommandFromRemoteSite(final ReplicableCommand cmd, final Message req, final org.jgroups.blocks.Response response) throws Throwable {
      SiteAddress siteAddress = (SiteAddress) req.getSrc();
      ((XSiteReplicateCommand) cmd).setOriginSite(siteAddress.getSite());
      Reply reply = returnValue -> CommandAwareRpcDispatcher.this.reply(response, returnValue, cmd, req);
      handler.handleFromRemoteSite(siteAddress.getSite(), (XSiteReplicateCommand) cmd, reply, decodeDeliverMode(req));
   }

   private void executeCommandFromLocalCluster(final ReplicableCommand cmd, final Message req, final org.jgroups.blocks.Response response) throws Throwable {
      Reply reply = returnValue -> CommandAwareRpcDispatcher.this.reply(response, returnValue, cmd, req);
      handler.handleFromCluster(fromJGroupsAddress(req.getSrc()), cmd, reply, decodeDeliverMode(req));
   }

   private static DeliverOrder decodeDeliverMode(Message request) {
      boolean noTotalOrder = request.isFlagSet(Message.Flag.NO_TOTAL_ORDER);
      boolean oob = request.isFlagSet(Message.Flag.OOB);
      if (!noTotalOrder && oob) {
         return DeliverOrder.TOTAL;
      } else if (noTotalOrder && oob) {
         return DeliverOrder.NONE;
      } else if (noTotalOrder) {
         //oob is not set at this point, but the no total order flag should.
         return DeliverOrder.PER_SENDER;
      }
      throw new IllegalArgumentException("Unable to decode message " + request);
   }

   private static void encodeDeliverMode(Message request, DeliverOrder deliverOrder) {
      switch (deliverOrder) {
         case TOTAL:
            request.setFlag(Message.Flag.OOB.value());
            break;
         case PER_SENDER:
            request.setFlag(Message.Flag.NO_TOTAL_ORDER.value());
            break;
         case NONE:
            request.setFlag((short) (Message.Flag.OOB.value() | Message.Flag.NO_TOTAL_ORDER.value()));
            break;
         default:
            throw new IllegalArgumentException("Unsupported deliver mode " + deliverOrder);
      }
   }

   private static void encodeDeliverMode(RequestOptions options, DeliverOrder deliverOrder) {
      switch (deliverOrder) {
         case TOTAL:
            options.flags(Message.Flag.OOB);
            break;
         case PER_SENDER:
            options.flags(Message.Flag.NO_TOTAL_ORDER);
            break;
         case NONE:
            options.flags(Message.Flag.OOB, Message.Flag.NO_TOTAL_ORDER);
            break;
         default:
            throw new IllegalArgumentException("Unsupported deliver mode " + deliverOrder);
      }
   }

   @Override
   public String toString() {
      return getClass().getSimpleName() + "[marshaller: " + streamMarshalling + "]";
   }

   private void reply(org.jgroups.blocks.Response response, Object retVal, ReplicableCommand command,
         Message req) {
      if (response != null) {
         if (trace) log.tracef("About to send back response %s for command %s", retVal, command);
         ByteBuffer rsp_buf;
         boolean is_exception = false;
         try {
            rsp_buf = streamMarshalling.objectToBuffer(retVal);
         } catch (Throwable t) {
            try {  // this call should succeed (all exceptions are serializable)
               rsp_buf = streamMarshalling.objectToBuffer(t);
               is_exception = true;
            } catch (Throwable tt) {
               log.errorMarshallingObject(tt, retVal);
               return;
            }
         }

         // Always set the NO_FC flag
         short flags = (short) (req.getFlags() | REPLY_FLAGS_TO_SET & ~REPLY_FLAGS_TO_CLEAR);
         Message rsp = req.makeReply().setFlag(flags).setBuffer(rsp_buf.getBuf(), rsp_buf.getOffset(), rsp_buf.getLength());

         //exceptionThrown is always false because the exceptions are wrapped in an ExceptionResponse
         response.send(rsp, is_exception);
      }
   }

   protected static Message constructMessage(Buffer buf, Address recipient,ResponseMode mode, boolean rsvp,
                                             DeliverOrder deliverOrder) {
      Message msg = new Message();
      msg.setBuffer(buf);
      encodeDeliverMode(msg, deliverOrder);
      //some issues with the new bundler. put back the DONT_BUNDLE flag.
      if (deliverOrder == DeliverOrder.NONE || mode != ResponseMode.GET_NONE) {
         msg.setFlag(Message.Flag.DONT_BUNDLE.value());
      }
      // Only the commands in total order must be received by the originator.
      if (deliverOrder != DeliverOrder.TOTAL) {
         msg.setTransientFlag(Message.TransientFlag.DONT_LOOPBACK.value());
      }
      if (rsvp) {
         msg.setFlag(Message.Flag.RSVP.value());
      }

      if (recipient != null) msg.setDest(recipient);
      return msg;
   }

   public static RequestOptions constructRequestOptions(ResponseMode mode, boolean rsvp, DeliverOrder deliverOrder,
                                                        long timeout) {
      RequestOptions options = new RequestOptions(mode, timeout);
      encodeDeliverMode(options, deliverOrder);
      //some issues with the new bundler. put back the DONT_BUNDLE flag.
      if (deliverOrder == DeliverOrder.NONE || mode != ResponseMode.GET_NONE) {
         options.flags(Message.Flag.DONT_BUNDLE);
      }
      // Only the commands in total order must be received by the originator.
      if (deliverOrder != DeliverOrder.TOTAL) {
         options.transientFlags(Message.TransientFlag.DONT_LOOPBACK);
      }
      if (rsvp) {
         options.flags(Message.Flag.RSVP);
      }
      return options;
   }

   ByteBuffer marshallCall(ReplicableCommand command) {
      ByteBuffer buf;
      try {
         buf = streamMarshalling.objectToBuffer(command);
      } catch (RuntimeException e) {
         throw e;
      } catch (Exception e) {
         throw new RuntimeException("Failure to marshal argument(s)", e);
      }
      return buf;
   }

   private SingleResponseFuture processSingleCall(ReplicableCommand command, long timeout,
                                                  Address destination, ResponseMode mode,
                                                  DeliverOrder deliverOrder, StreamingMarshaller marshaller) throws Exception {
      if (trace)
         log.tracef("Replication task sending %s to single recipient %s with response mode %s", command, destination, mode);
      boolean rsvp = isRsvpCommand(command);

      // Replay capability requires responses from all members!
      ByteBuffer buf = marshallCall(command);
      RequestOptions options = constructRequestOptions(mode, rsvp, deliverOrder, timeout);
      CompletableFuture<Response> request = sendMessageWithFuture(destination, buf.getBuf(), buf.getOffset(), buf.getLength(), options);
      if (mode == ResponseMode.GET_NONE)
         return null;

      SingleResponseFuture retval = new SingleResponseFuture(request);
      if (timeout > 0 ) {
         retval.setTimeoutTask(timeoutExecutor, timeout);
      }
      return retval;
   }

   private void processCallsStaggered(ReplicableCommand command, RspFilter filter, List<Address> dests,
                                      ResponseMode mode, DeliverOrder deliverOrder, StreamingMarshaller marshaller,
                                      CompletableFuture<RspList<Response>> theFuture, int destIndex,
                                      long deadline, RspList<Response> rsps)
         throws Exception {
      if (destIndex == dests.size())
         return;

      final Address destination = dests.get(destIndex);
      SingleResponseFuture subFuture = processSingleCall(command, -1, destination, mode, deliverOrder, marshaller);
      if (subFuture != null) {
         subFuture.whenComplete((rsp, throwable) -> {
            if (throwable != null) {
               // We should never get here, any remote exception will be in the Rsp
               theFuture.completeExceptionally(throwable);
            }
            Rsp<Response> futureRsp = rsps.get(destination);
            if (rsp.hasException()) {
               futureRsp.setException(rsp.getException());
            } else {
               futureRsp.setValue(rsp.getValue());
            }
            if (filter.isAcceptable(rsp.getValue(), destination)) {
               // We got an acceptable response
               theFuture.complete(rsps);
            } else {
               // We only give up after we've received invalid responses from all recipients,
               // even after the stagger timeout expired for them.
               boolean missingResponses = false;
               for (Rsp<Response> rsp1 : rsps) {
                  if (!rsp1.wasReceived()) {
                     missingResponses = true;
                     break;
                  }
               }
               if (!missingResponses) {
                  // This was the last response, need to complete the future
                  theFuture.complete(rsps);
               } else {
                  // The response was not acceptable, complete the timeout future to start the next request
                  staggeredProcessNext(command, filter, dests, mode, deliverOrder, marshaller, theFuture,
                        destIndex, deadline, rsps);
               }
            }
         });
         if (!subFuture.isDone()) {
            long delayNanos = timeService.remainingTime(deadline, TimeUnit.NANOSECONDS);
            if (destIndex < dests.size() - 1) {
               // Not the last recipient, wait for STAGGER_DELAY_NANOS only
               delayNanos = Math.min(STAGGER_DELAY_NANOS, delayNanos);
            }
            ScheduledFuture<?> timeoutTask = timeoutExecutor.schedule(
                  () -> staggeredProcessNext(command, filter, dests, mode, deliverOrder, marshaller,
                        theFuture, destIndex, deadline, rsps), delayNanos, TimeUnit.NANOSECONDS);
            theFuture.whenComplete((rsps1, throwable) -> timeoutTask.cancel(false));
         }
      } else {
         staggeredProcessNext(command, filter, dests, mode, deliverOrder, marshaller, theFuture, destIndex,
               deadline, rsps);
      }
   }

   private void staggeredProcessNext(ReplicableCommand command, RspFilter filter, List<Address> dests,
         ResponseMode mode, DeliverOrder deliverOrder, StreamingMarshaller marshaller,
         CompletableFuture<RspList<Response>> theFuture, int destIndex, long deadline,
         RspList<Response> rsps) {
      if (theFuture.isDone()) {
         return;
      }
      if (timeService.isTimeExpired(deadline)) {
         theFuture.complete(rsps);
         return;
      }
      try {
         processCallsStaggered(command, filter, dests, mode, deliverOrder, marshaller, theFuture,
               destIndex + 1, deadline, rsps);
      } catch (Exception e) {
         // We should never get here, any remote exception will be in the Rsp
         theFuture.completeExceptionally(e);
      }
   }

   private RspListFuture processCalls(ReplicableCommand command, boolean broadcast, long timeout,
                                      RspFilter filter, List<Address> dests, ResponseMode mode,
                                      DeliverOrder deliverOrder, StreamingMarshaller marshaller) throws Exception {
      if (trace) log.tracef("Replication task sending %s to addresses %s with response mode %s", command, dests, mode);
      boolean rsvp = isRsvpCommand(command);

      ByteBuffer buf = marshallCall(command);
      RequestOptions opts = constructRequestOptions(mode, rsvp, deliverOrder, timeout);
      Collection<Address> realDest = dests;
      if (deliverOrder == DeliverOrder.TOTAL) {
         opts.anycasting(true).useAnycastAddresses(true);
      } else if (broadcast || FORCE_MCAST) {
         opts.anycasting(false);
         realDest = null; //broadcast
      } else {
         opts.anycasting(true).setUseAnycastAddresses(false);
      }
      opts.rspFilter(filter);

      GroupRequest<Response> request = cast(realDest, buf.getBuf(), buf.getOffset(), buf.getLength(), opts, false);
      if (mode == ResponseMode.GET_NONE)
         return null;

      RspListFuture retVal = new RspListFuture(broadcast ? null : dests, request);
      if (timeout > 0) {
         retVal.setTimeoutTask(timeoutExecutor, timeout);
      }
      return retVal;
   }

   private static boolean isRsvpCommand(ReplicableCommand command) {
      return command instanceof FlagAffectedCommand
            && ((FlagAffectedCommand) command).hasFlag(Flag.GUARANTEED_DELIVERY);
   }

}
