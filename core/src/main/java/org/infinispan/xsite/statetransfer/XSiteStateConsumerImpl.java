package org.infinispan.xsite.statetransfer;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.context.SingleKeyNonTxInvocationContext;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.statetransfer.KeyTrackAndCommitter;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.TransactionManager;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;

import static org.infinispan.context.Flag.*;

/**
 * It contains the logic needed to consume the state sent from other site.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class XSiteStateConsumerImpl implements XSiteStateConsumer {

   private static final EnumSet<Flag> STATE_TRANSFER_PUT_FLAGS = EnumSet.of(PUT_FOR_X_SITE_STATE_TRANSFER,
                                                                            IGNORE_RETURN_VALUES, SKIP_REMOTE_LOOKUP,
                                                                            SKIP_XSITE_BACKUP);
   private static final Log log = LogFactory.getLog(XSiteStateConsumerImpl.class);
   private static final boolean trace = log.isTraceEnabled();
   private static final boolean debug = log.isDebugEnabled();

   private TransactionManager transactionManager;
   private InvocationContextFactory invocationContextFactory;
   private CommandsFactory commandsFactory;
   private InterceptorChain interceptorChain;
   private ClusteringDependentLogic clusteringDependentLogic;
   private KeyTrackAndCommitter keyTrackAndCommitter;

   @Inject
   public void inject(TransactionManager transactionManager, InvocationContextFactory invocationContextFactory,
                      CommandsFactory commandsFactory, InterceptorChain interceptorChain,
                      ClusteringDependentLogic clusteringDependentLogic, KeyTrackAndCommitter keyTrackAndCommitter) {
      this.transactionManager = transactionManager;
      this.invocationContextFactory = invocationContextFactory;
      this.commandsFactory = commandsFactory;
      this.interceptorChain = interceptorChain;
      this.clusteringDependentLogic = clusteringDependentLogic;
      this.keyTrackAndCommitter = keyTrackAndCommitter;
   }

   @Override
   public void startStateTransfer() {
      if (debug) {
         log.debugf("Starting state transfer.");
      }
      keyTrackAndCommitter.startTrack(Flag.PUT_FOR_X_SITE_STATE_TRANSFER);
   }

   @Override
   public void endStateTransfer() {
      if (debug) {
         log.debugf("Ending state transfer.");
      }
      keyTrackAndCommitter.stopTrack(Flag.PUT_FOR_X_SITE_STATE_TRANSFER);
   }

   @Override
   public void applyState(XSiteState[] chunk) throws Exception {
      if (trace) {
         log.tracef("Received state: %s", extractKeys(chunk));
      } else if (debug) {
         log.debugf("Received state: %s keys", chunk.length);
      }
      if (transactionManager != null) {
         applyStateInTransaction(chunk);
      } else {
         applyStateInNonTransaction(chunk);
      }
   }

   private void applyStateInTransaction(XSiteState[] chunk) throws Exception {
      List<Object> keysInserted = null;
      int keysCounter = 0;
      if (trace) {
         keysInserted = new ArrayList<Object>(chunk.length);
      }
      try {
         transactionManager.begin();
         InvocationContext ctx = invocationContextFactory.createInvocationContext(transactionManager.getTransaction(),
                                                                                  true);
         ((TxInvocationContext) ctx).getCacheTransaction().setStateTransferFlag(PUT_FOR_X_SITE_STATE_TRANSFER);
         for (XSiteState siteState : chunk) {
            if (shouldPut(siteState.key())) {
               interceptorChain.invoke(ctx, createPut(siteState));
               if (trace) {
                  keysInserted.add(siteState.key());
               } else if (debug) {
                  keysCounter++;
               }
            }
         }
         transactionManager.commit();
         if (trace) {
            log.tracef("Successfully applied state. Inserted keys=%s.%nTotal keys=%s", keysInserted,
                       extractKeys(chunk));
         } else if (debug) {
            log.debugf("Successfully applied state. Inserted: %s keys (%s total)", keysCounter, chunk.length);
         }
      } catch (Exception e) {
         if (trace) {
            log.unableToApplyXSiteStateT(keysInserted, extractKeys(chunk), e);
         } else if (debug) {
            log.unableToApplyXSiteStateD(keysCounter, chunk.length, e);
         } else {
            log.unableToApplyXSiteState(e);
         }
         safeRollback();
         throw e;
      }
   }

   private void applyStateInNonTransaction(XSiteState[] chunk) {
      SingleKeyNonTxInvocationContext ctx = (SingleKeyNonTxInvocationContext) invocationContextFactory
            .createSingleKeyNonTxInvocationContext();

      List<Object> keysInserted = null;
      int keysCounter = 0;
      if (trace) {
         keysInserted = new ArrayList<Object>(chunk.length);
      }

      for (XSiteState siteState : chunk) {
         if (shouldPut(siteState.key())) {
            interceptorChain.invoke(ctx, createPut(siteState));
            ctx.resetState(); //re-use same context. Old context is not longer needed
            if (trace) {
               keysInserted.add(siteState.key());
            } else if (debug) {
               keysCounter++;
            }
         }
      }
      if (trace) {
         log.tracef("Successfully applied state. Inserted keys=%s.%nTotal keys=%s", keysInserted,
                    extractKeys(chunk));
      } else if (debug) {
         log.debugf("Successfully applied state. Inserted: %s keys (%s total)", keysCounter, chunk.length);
      }
   }

   private boolean shouldPut(Object key) {
      return clusteringDependentLogic.localNodeIsPrimaryOwner(key);
   }

   private PutKeyValueCommand createPut(XSiteState state) {
      return commandsFactory.buildPutKeyValueCommand(state.key(), state.value(), state.metadata(),
                                                     STATE_TRANSFER_PUT_FLAGS);
   }

   private void safeRollback() {
      try {
         transactionManager.rollback();
      } catch (Exception e) {
         //ignored!
         if (debug) {
            log.debug("Error rollbacking transaction.", e);
         }
      }
   }

   private Collection<Object> extractKeys(XSiteState[] chunk) {
      List<Object> keys = new ArrayList<Object>(chunk.length);
      for (XSiteState aChunk : chunk) {
         keys.add(aChunk.key());
      }
      return keys;
   }
}
