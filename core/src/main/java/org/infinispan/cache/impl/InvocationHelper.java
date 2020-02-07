package org.infinispan.cache.impl;

import java.util.concurrent.CompletableFuture;

import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.infinispan.batch.BatchContainer;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.TransactionConfiguration;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.locks.RemoteLockCommand;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * //TODO document this!
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
@Scope(Scopes.NAMED_CACHE)
public class InvocationHelper {

   private static final Log log = LogFactory.getLog(InvocationHelper.class);
   private static final boolean trace = log.isTraceEnabled();

   @Inject
   protected AsyncInterceptorChain invoker;
   @Inject
   protected InvocationContextFactory invocationContextFactory;
   @Inject
   protected TransactionManager transactionManager;
   @Inject
   protected Configuration config;
   @Inject
   protected BatchContainer batchContainer;
   private final ContextBuilder defaultBuilder = i -> createInvocationContextWithImplicitTransaction(i, false);


   private static void checkLockOwner(InvocationContext context, VisitableCommand command) {
      if (context.getLockOwner() == null && command instanceof RemoteLockCommand) {
         context.setLockOwner(((RemoteLockCommand) command).getKeyLockOwner());
      }
   }

   private static boolean isTxInjected(InvocationContext ctx) {
      //noinspection rawtypes
      return ctx.isInTxScope() && ((TxInvocationContext) ctx).isImplicitTransaction();
   }


   public <T> T invoke(VisitableCommand command, int keyCount) {
      InvocationContext ctx = createInvocationContextWithImplicitTransaction(keyCount, false);
      return invoke(ctx, command);
   }

   public <T> T invoke(ContextBuilder builder, VisitableCommand command, int keyCount) {
      InvocationContext ctx = builder.create(keyCount);
      return invoke(ctx, command);
   }


   public <T> T invoke(InvocationContext context, VisitableCommand command) {
      checkLockOwner(context, command);
      //noinspection unchecked
      return isTxInjected(context) ?
            (T) executeCommandWithInjectedTx(context, command) :
            (T) invoker.invoke(context, command);
   }

   public <T> CompletableFuture<T> invokeAsync(VisitableCommand command, int keyCount) {
      InvocationContext ctx = createInvocationContextWithImplicitTransaction(keyCount, false);
      return invokeAsync(ctx, command);
   }

   public <T> CompletableFuture<T> invokeAsync(ContextBuilder builder, VisitableCommand command, int keyCount) {
      InvocationContext ctx = builder.create(keyCount);
      return invokeAsync(ctx, command);
   }

   public <T> CompletableFuture<T> invokeAsync(InvocationContext context, VisitableCommand command) {
      checkLockOwner(context, command);
      //noinspection unchecked
      return isTxInjected(context) ?
            executeCommandAsyncWithInjectedTx(context, command) :
            (CompletableFuture<T>) invoker.invokeAsync(context, command);
   }


   public ContextBuilder defaultContextBuilderForWrite() {
      return defaultBuilder;
   }

   /**
    * Creates an invocation context with an implicit transaction if it is required. An implicit transaction is created
    * if there is no current transaction and autoCommit is enabled.
    *
    * @param keyCount how many keys are expected to be changed
    * @return the invocation context
    */
   public InvocationContext createInvocationContextWithImplicitTransaction(int keyCount,
         boolean forceCreateTransaction) {
      boolean txInjected = false;
      TransactionConfiguration txConfig = config.transaction();
      if (txConfig.transactionMode().isTransactional()) {
         Transaction transaction = getOngoingTransaction(true);
         if (transaction == null && (forceCreateTransaction || txConfig.autoCommit())) {
            transaction = tryBegin();
            txInjected = true;
         }
         return invocationContextFactory.createInvocationContext(transaction, txInjected);
      } else {
         return invocationContextFactory.createInvocationContext(true, keyCount);
      }
   }

   private Transaction getOngoingTransaction(boolean includeBatchTx) {
      try {
         Transaction transaction = null;
         if (transactionManager != null) {
            transaction = transactionManager.getTransaction();
            if (includeBatchTx && transaction == null && config.invocationBatching().enabled()) {
               transaction = batchContainer.getBatchTransaction();
            }
         }
         return transaction;
      } catch (SystemException e) {
         throw new CacheException("Unable to get transaction", e);
      }
   }

   private Object executeCommandWithInjectedTx(InvocationContext ctx, VisitableCommand command) {
      final Object result;
      try {
         result = invoker.invoke(ctx, command);
      } catch (Throwable e) {
         tryRollback();
         throw e;
      }
      tryCommit();
      return result;
   }

   private <T> CompletableFuture<T> executeCommandAsyncWithInjectedTx(InvocationContext ctx, VisitableCommand command) {
      CompletableFuture<T> cf;
      final Transaction implicitTransaction;
      try {
         // interceptors must not access thread-local transaction anyway
         implicitTransaction = transactionManager.suspend();
         assert implicitTransaction != null;
         //noinspection unchecked
         cf = (CompletableFuture<T>) invoker.invokeAsync(ctx, command);
      } catch (SystemException e) {
         throw new CacheException("Cannot suspend implicit transaction", e);
      } catch (Throwable e) {
         tryRollback();
         throw e;
      }
      return cf.handle((result, throwable) -> {
         if (throwable != null) {
            try {
               implicitTransaction.rollback();
            } catch (SystemException e) {
               log.trace("Could not rollback", e);
               throwable.addSuppressed(e);
            }
            throw CompletableFutures.asCompletionException(throwable);
         }
         try {
            implicitTransaction.commit();
         } catch (Exception e) {
            log.couldNotCompleteInjectedTransaction(e);
            throw CompletableFutures.asCompletionException(e);
         }
         return result;
      });
   }

   private Transaction tryBegin() {
      if (transactionManager == null) {
         return null;
      }
      try {
         transactionManager.begin();
         final Transaction transaction = getOngoingTransaction(true);
         if (trace) {
            log.tracef("Implicit transaction started! Transaction: %s", transaction);
         }
         return transaction;
      } catch (RuntimeException e) {
         throw e;
      } catch (Exception e) {
         throw new CacheException("Unable to begin implicit transaction.", e);
      }
   }

   private void tryRollback() {
      try {
         if (transactionManager != null) {
            transactionManager.rollback();
         }
      } catch (Throwable t) {
         if (trace) {
            log.trace("Could not rollback", t);//best effort
         }
      }
   }

   private void tryCommit() {
      if (transactionManager == null) {
         return;
      }
      if (trace) {
         log.tracef("Committing transaction as it was implicit: %s", getOngoingTransaction(true));
      }
      try {
         transactionManager.commit();
      } catch (Throwable e) {
         log.couldNotCompleteInjectedTransaction(e);
         throw new CacheException("Could not commit implicit transaction", e);
      }
   }
}
