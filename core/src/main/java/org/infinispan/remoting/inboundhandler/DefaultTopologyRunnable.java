package org.infinispan.remoting.inboundhandler;

import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.Response;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public class DefaultTopologyRunnable extends BaseBlockingRunnable {

   private final TopologyMode topologyMode;
   private final int commandTopologyId;

   public DefaultTopologyRunnable(BasePerCacheInboundInvocationHandler handler, CacheRpcCommand command, Reply reply,
                                  TopologyMode topologyMode, int commandTopologyId) {
      super(handler, command, reply);
      this.topologyMode = topologyMode;
      this.commandTopologyId = commandTopologyId;
   }

   @Override
   public boolean isReady() {
      return topologyMode.isReady(handler.getStateTransferLock(), waitTopology());
   }

   @Override
   protected Response beforeInvoke() throws Exception {
      if (0 <= commandTopologyId && commandTopologyId < handler.stateTransferManager.getFirstTopologyAsMember()) {
         if (handler.isTraceEnabled()) {
            handler.getLog().tracef("Ignoring command sent before the local node was a member " +
                                          "(command topology id is %d)", commandTopologyId);
         }
         return CacheNotFoundResponse.INSTANCE;
      }
      topologyMode.await(handler.getStateTransferLock(), waitTopology());
      return super.beforeInvoke();
   }

   private int waitTopology() {
      // Always wait for the first topology (i.e. for the join to finish)
      return Math.max(commandTopologyId, 0);
   }

}
