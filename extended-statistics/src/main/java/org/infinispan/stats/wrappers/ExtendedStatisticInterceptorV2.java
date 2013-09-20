package org.infinispan.stats.wrappers;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.interceptors.base.BaseCustomInterceptor;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.remoting.RemoteException;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.stats.container.DataAccessStatisticsContainer;
import org.infinispan.stats.container.transactional.TransactionStatisticsContainer;
import org.infinispan.stats.logging.Log;
import org.infinispan.stats.manager.StatisticsManager;
import org.infinispan.stats.manager.TransactionalStatisticsManager;
import org.infinispan.transaction.WriteSkewException;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.TimeService;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.locks.DeadlockDetectedException;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.logging.LogFactory;

/**
 * Take the statistics about relevant visitable commands.
 *
 * @author Roberto Palmieri
 * @author Sebastiano Peluso
 * @author Diego Didona
 * @author Pedro Ruivo
 * @since 6.0
 */
@MBean(objectName = "ExtendedStatisticsV2", description = "Component that manages and exposes extended statistics " +
      "relevant to transactions.")
public class ExtendedStatisticInterceptorV2 extends BaseCustomInterceptor {

   private static final Log log = LogFactory.getLog(ExtendedStatisticInterceptorV2.class, Log.class);
   private RpcManager rpcManager;
   private StatisticsManager statisticsManager;
   private TimeService timeService;

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      DataAccessStatisticsContainer container = getDataAccessStatisticsContainer(ctx);
      if (container != null) {
         return visitWriteOperation(ctx, command, container);
      }
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      DataAccessStatisticsContainer container = getDataAccessStatisticsContainer(ctx);
      if (container != null) {
         return visitWriteOperation(ctx, command, container);
      }
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      DataAccessStatisticsContainer container = getDataAccessStatisticsContainer(ctx);
      if (container != null) {
         return visitWriteOperation(ctx, command, container);
      }
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      DataAccessStatisticsContainer container = getDataAccessStatisticsContainer(ctx);
      if (container != null) {
         return visitWriteOperation(ctx, command, container);
      }
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      DataAccessStatisticsContainer container = getDataAccessStatisticsContainer(ctx);
      if (container != null) {
         return visitWriteOperation(ctx, command, container);
      }
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      DataAccessStatisticsContainer container = getDataAccessStatisticsContainer(ctx);
      if (container != null) {
         VisitableCommandStats stats = visitVisitableCommandWithStats(ctx, command);
         if (stats.throwable != null) {
            switch (stats.cause) {
               case LOCK_TIMEOUT:
                  container.readAccessLockTimeout(stats.duration);
                  break;
               case NETWORK_TIMEOUT:
                  container.readAccessNetworkTimeout(stats.duration);
                  break;
               case DEADLOCK_DETECTED:
                  container.readAccessDeadlock(stats.duration);
                  break;
               case VALIDATION_FAILED:
                  container.readAccessValidationError(stats.duration);
                  break;
               default:
                  container.readAccessUnknownError(stats.duration);
            }
            throw stats.throwable;
         }
         container.readAccess(stats.duration);
         return stats.returnValue;
      }
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      TransactionStatisticsContainer container = getTransactionStatisticsContainer(ctx);
      if (container != null) {
         VisitableCommandStats stats = visitVisitableCommandWithStats(ctx, command);
         if (stats.throwable != null) {
            switch (stats.cause) {
               case LOCK_TIMEOUT:
                  container.prepareLockTimeout(stats.duration);
                  break;
               case NETWORK_TIMEOUT:
                  container.prepareNetworkTimeout(stats.duration);
                  break;
               case DEADLOCK_DETECTED:
                  container.prepareDeadlockError(stats.duration);
                  break;
               case VALIDATION_FAILED:
                  container.prepareValidationError(stats.duration);
                  break;
               default:
                  container.prepareUnknownError(stats.duration);
            }
            throw stats.throwable;
         }
         container.prepare(stats.duration);
         return stats.returnValue;
      }
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      TransactionStatisticsContainer container = getTransactionStatisticsContainer(ctx);
      if (container != null) {
         VisitableCommandStats stats = visitVisitableCommandWithStats(ctx, command);
         if (stats.throwable != null) {
            switch (stats.cause) {
               case NETWORK_TIMEOUT:
                  container.commitNetworkTimeout(stats.duration);
                  break;
               default:
                  container.commitUnknownError(stats.duration);
            }
            throw stats.throwable;
         }
         container.commit(stats.duration);
         return stats.returnValue;
      }
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      TransactionStatisticsContainer container = getTransactionStatisticsContainer(ctx);
      if (container != null) {
         VisitableCommandStats stats = visitVisitableCommandWithStats(ctx, command);
         if (stats.throwable != null) {
            switch (stats.cause) {
               case NETWORK_TIMEOUT:
                  container.rollbackNetworkTimeout(stats.duration);
                  break;
               default:
                  container.rollbackUnknownError(stats.duration);
            }
            throw stats.throwable;
         }
         container.rollback(stats.duration);
         return stats.returnValue;
      }
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   protected void start() {
      super.start();
      log.startExtendedStatisticInterceptor();
      this.timeService = cache.getAdvancedCache().getComponentRegistry().getTimeService();
      this.statisticsManager = new TransactionalStatisticsManager(timeService, rpcManager);
      replace();
   }

   private DataAccessStatisticsContainer getDataAccessStatisticsContainer(InvocationContext context) {
      final GlobalTransaction globalTransaction = context.isInTxScope() ?
            ((TxInvocationContext) context).getGlobalTransaction() :
            null;
      return statisticsManager.getDataAccessStatisticsContainer(globalTransaction, context.isOriginLocal());
   }

   private Object visitWriteOperation(InvocationContext context, VisitableCommand command,
                                      DataAccessStatisticsContainer container) throws Throwable {
      VisitableCommandStats stats = visitVisitableCommandWithStats(context, command);
      if (stats.throwable != null) {
         switch (stats.cause) {
            case LOCK_TIMEOUT:
               container.writeAccessLockTimeout(stats.duration);
               break;
            case NETWORK_TIMEOUT:
               container.writeAccessNetworkTimeout(stats.duration);
               break;
            case DEADLOCK_DETECTED:
               container.writeAccessDeadlock(stats.duration);
               break;
            case VALIDATION_FAILED:
               container.writeAccessValidationError(stats.duration);
               break;
            default:
               container.writeAccessUnknownError(stats.duration);
         }
         throw stats.throwable;
      }
      container.writeAccess(stats.duration);
      return stats.returnValue;
   }

   private VisitableCommandStats visitVisitableCommandWithStats(InvocationContext ctx, VisitableCommand command) {
      if (log.isTraceEnabled()) {
         log.tracef("Visit command %s. Is it in transaction scope? %s. Is it local? %s", command,
                    ctx.isInTxScope(), ctx.isOriginLocal());
      }
      final VisitableCommandStats stats = new VisitableCommandStats();
      final long start = timeService.time();
      try {
         stats.returnValue = invokeNextInterceptor(ctx, command);
      } catch (TimeoutException e) {
         stats.throwable = e;
         if (isLockTimeout(e)) {
            stats.cause = WriteCommandExceptionCause.LOCK_TIMEOUT;
         } else if (isNetworkTimeout(e)) {
            stats.cause = WriteCommandExceptionCause.NETWORK_TIMEOUT;
         } else {
            stats.cause = WriteCommandExceptionCause.UNKNOWN;
         }
      } catch (DeadlockDetectedException e) {
         stats.throwable = e;
         stats.cause = WriteCommandExceptionCause.DEADLOCK_DETECTED;
      } catch (WriteSkewException e) {
         stats.throwable = e;
         stats.cause = WriteCommandExceptionCause.VALIDATION_FAILED;
      } catch (RemoteException remote) {
         stats.throwable = remote;
         stats.cause = WriteCommandExceptionCause.UNKNOWN;
         Throwable cause = remote.getCause();
         while (cause != null) {
            if (cause instanceof TimeoutException) {
               if (isLockTimeout((TimeoutException) cause)) {
                  stats.cause = WriteCommandExceptionCause.LOCK_TIMEOUT;
               } else if (isNetworkTimeout((TimeoutException) cause)) {
                  stats.cause = WriteCommandExceptionCause.NETWORK_TIMEOUT;
               } else {
                  stats.cause = WriteCommandExceptionCause.UNKNOWN;
               }
               break;
            } else if (cause instanceof DeadlockDetectedException) {
               stats.cause = WriteCommandExceptionCause.DEADLOCK_DETECTED;
               break;
            } else if (cause instanceof WriteSkewException) {
               stats.cause = WriteCommandExceptionCause.VALIDATION_FAILED;
               break;
            }
            cause = cause.getCause();
         }
      } catch (Throwable throwable) {
         stats.throwable = throwable;
         stats.cause = WriteCommandExceptionCause.UNKNOWN;
      } finally {
         stats.duration = timeService.time() - start;
      }
      return stats;
   }

   private TransactionStatisticsContainer getTransactionStatisticsContainer(TxInvocationContext context) {
      return statisticsManager.getTransactionStatisticsContainer(context.getGlobalTransaction(),
                                                                 context.isOriginLocal());
   }

   private void replace() {
      log.replaceComponents();
      ComponentRegistry componentRegistry = cache.getAdvancedCache().getComponentRegistry();

      replaceRpcManager(componentRegistry);
      replaceLockManager(componentRegistry);
      componentRegistry.rewire();
   }

   private void replaceLockManager(ComponentRegistry componentRegistry) {
      LockManager oldLockManager = componentRegistry.getComponent(LockManager.class);
      LockManager newLockManager = new ExtendedStatisticLockManagerv2(oldLockManager, statisticsManager, timeService);
      log.replaceComponent("LockManager", oldLockManager, newLockManager);
      componentRegistry.registerComponent(newLockManager, LockManager.class);
   }

   private void replaceRpcManager(ComponentRegistry componentRegistry) {
      RpcManager oldRpcManager = componentRegistry.getComponent(RpcManager.class);
      if (oldRpcManager == null) {
         //local mode
         return;
      }
      RpcManager newRpcManager = new ExtendedStatisticRpcManagerv2(oldRpcManager, statisticsManager, timeService);
      log.replaceComponent("RpcManager", oldRpcManager, newRpcManager);
      componentRegistry.registerComponent(newRpcManager, RpcManager.class);
      this.rpcManager = newRpcManager;
   }

   private boolean isLockTimeout(TimeoutException e) {
      return e.getMessage().startsWith("Unable to acquire lock after");
   }

   private boolean isNetworkTimeout(TimeoutException e) {
      final String message = e.getMessage();
      return message.startsWith("Timed out waiting for valid responses") ||
            message.startsWith("Replication timeout");
   }

   private enum WriteCommandExceptionCause {
      LOCK_TIMEOUT, DEADLOCK_DETECTED, NETWORK_TIMEOUT, VALIDATION_FAILED, UNKNOWN
   }

   private class VisitableCommandStats {
      private Object returnValue;
      private Throwable throwable;
      private WriteCommandExceptionCause cause;
      private long duration;
   }
}
