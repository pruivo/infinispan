package org.infinispan.remoting.inboundhandler.action;

import org.infinispan.commands.write.BackupWriteRcpCommand;
import org.infinispan.distribution.TriangleOrderManager;
import org.infinispan.remoting.inboundhandler.NonTotalOrderPerCacheInboundInvocationHandler;

/**
 * //TODO document this!
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class TriangleOrderAction implements Action {

   private static final String SEQUENCE_ID_KEY = "__sid__";
   private final NonTotalOrderPerCacheInboundInvocationHandler handler;

   public TriangleOrderAction(NonTotalOrderPerCacheInboundInvocationHandler handler) {
      this.handler = handler;
   }

   @Override
   public ActionStatus check(ActionState state) {
      final BackupWriteRcpCommand command = state.getCommand();
      final TriangleOrderManager triangleOrderManager = handler.getTriangleOrderManager();
      int segmentId = (int) state.getState()
            .computeIfAbsent(SEQUENCE_ID_KEY, s -> triangleOrderManager.calculateSegmentId(command.getKey()));
      return triangleOrderManager.isNext(segmentId, command.getTopologyId(), command.getSequence()) ?
            ActionStatus.READY :
            ActionStatus.NOT_READY;
   }

   @Override
   public void onFinally(ActionState state) {
      final BackupWriteRcpCommand command = state.getCommand();
      final int segmentId = (int) state.getState().getOrDefault(SEQUENCE_ID_KEY, -1);
      handler.getTriangleOrderManager().markDelivered(segmentId, command.getTopologyId(), command.getSequence());
      handler.getRemoteExecutor().checkForReadyTasks();
   }
}
