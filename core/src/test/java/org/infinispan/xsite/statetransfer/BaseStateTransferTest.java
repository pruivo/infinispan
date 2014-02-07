package org.infinispan.xsite.statetransfer;

import org.infinispan.Cache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.Flag;
import org.infinispan.manager.CacheContainer;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.CommandAwareRpcDispatcher;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.statetransfer.KeyTrackAndCommitter;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.xsite.AbstractTwoSitesTest;
import org.infinispan.xsite.BackupReceiverRepository;
import org.infinispan.xsite.BackupReceiverRepositoryDelegate;
import org.infinispan.xsite.XSiteAdminOperations;
import org.jgroups.protocols.relay.SiteAddress;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.infinispan.test.TestingUtil.*;
import static org.testng.AssertJUnit.*;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public abstract class BaseStateTransferTest extends AbstractTwoSitesTest {

   protected static final String LON = "LON";
   protected static final String NYC = "NYC";

   public BaseStateTransferTest() {
      this.cleanup = CleanupPhase.AFTER_METHOD;
   }

   public void testStateTransferNonExistingSite() {
      XSiteAdminOperations operations = extractComponent(cache(LON, 0), XSiteAdminOperations.class);
      assertEquals("Unable to pushState to 'NO_SITE'. Incorrect site name: NO_SITE", operations.pushState("NO_SITE"));
      assertTrue(operations.getRunningStateTransfer().isEmpty());
      assertNoStateTransferInSendingSite(LON);
   }

   public void testStateTransferWithClusterIdle() throws InterruptedException {
      takeSiteOffline(LON, NYC);
      assertOffline(LON, NYC);
      assertNoStateTransferInReceivingSite(NYC);
      assertNoStateTransferInSendingSite(LON);

      //NYC is offline... lets put some initial data in
      //we have 2 nodes in each site and the primary owner sends the state. Lets try to have more key than the chunk
      //size in order to each site to send more than one chunk.
      final int amountOfData = chunkSize(LON) * 4;
      for (int i = 0; i < amountOfData; ++i) {
         cache(LON, 0).put(key(i), value(0));
      }

      //check if NYC is empty
      assertInSite(NYC, new AssertCondition<Object, Object>() {
         @Override
         public void assertInCache(Cache<Object, Object> cache) {
            assertTrue(cache.isEmpty());
         }
      });

      startStateTransfer(LON, NYC);

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return extractComponent(cache(LON, 0), XSiteAdminOperations.class).getRunningStateTransfer().isEmpty();
         }
      }, TimeUnit.SECONDS.toMillis(30));

      assertOnline(LON, NYC);

      //check if all data is visible
      assertInSite(NYC, new AssertCondition<Object, Object>() {
         @Override
         public void assertInCache(Cache<Object, Object> cache) {
            for (int i = 0; i < amountOfData; ++i) {
               assertEquals(value(0), cache.get(key(i)));
            }
         }
      });

      assertNoStateTransferInReceivingSite(NYC);
      assertNoStateTransferInSendingSite(LON);
   }

   public void testPutOperationBeforeState() throws Exception {
      testStateTransferWithConcurrentOperation(Operation.PUT, true);
   }

   public void testPutOperationAfterState() throws Exception {
      testStateTransferWithConcurrentOperation(Operation.PUT, false);
   }

   public void testRemoveOperationBeforeState() throws Exception {
      testStateTransferWithConcurrentOperation(Operation.REMOVE, true);
   }

   public void testRemoveOperationAfterState() throws Exception {
      testStateTransferWithConcurrentOperation(Operation.REMOVE, false);
   }

   public void testRemoveIfMatchOperationBeforeState() throws Exception {
      testStateTransferWithConcurrentOperation(Operation.REMOVE_IF_MATCH, true);
   }

   public void testRemoveIfMatchOperationAfterState() throws Exception {
      testStateTransferWithConcurrentOperation(Operation.REMOVE_IF_MATCH, false);
   }

   public void testReplaceOperationBeforeState() throws Exception {
      testStateTransferWithConcurrentOperation(Operation.REPLACE, true);
   }

   public void testReplaceOperationAfterState() throws Exception {
      testStateTransferWithConcurrentOperation(Operation.REPLACE, false);
   }

   public void testReplaceIfMatchOperationBeforeState() throws Exception {
      testStateTransferWithConcurrentOperation(Operation.REPLACE_IF_MATCH, true);
   }

   public void testReplaceIfMatchOperationAfterState() throws Exception {
      testStateTransferWithConcurrentOperation(Operation.REPLACE_IF_MATCH, false);
   }

   public void testClearOperationBeforeState() throws Exception {
      testStateTransferWithConcurrentOperation(Operation.CLEAR, true);
   }

   public void testClearOperationAfterState() throws Exception {
      testStateTransferWithConcurrentOperation(Operation.CLEAR, false);
   }

   public void testPutMapOperationBeforeState() throws Exception {
      testStateTransferWithConcurrentOperation(Operation.PUT_MAP, true);
   }

   public void testPutMapOperationAfterState() throws Exception {
      testStateTransferWithConcurrentOperation(Operation.PUT_MAP, false);
   }

   public void testPutIfAbsentFail() throws Exception {
      testStateTransferWithNoReplicatedOperation(Operation.PUT_IF_ABSENT_FAIL);
   }

   public void testRemoveIfMatchFail() throws Exception {
      testStateTransferWithNoReplicatedOperation(Operation.REMOVE_IF_MATCH_FAIL);
   }

   public void testReplaceIfMatchFail() throws Exception {
      testStateTransferWithNoReplicatedOperation(Operation.REPLACE_IF_MATCH_FAIL);
   }

   public void testPutIfAbsent() throws Exception {
      testConcurrentOperation(Operation.PUT_IF_ABSENT);
   }

   public void testRemoveNonExisting() throws Exception {
      testConcurrentOperation(Operation.REMOVE_NON_EXISTING);
   }

   private void testStateTransferWithConcurrentOperation(final Operation operation, final boolean performBeforeState)
         throws Exception {
      assertNotNull(operation);
      assertTrue(operation.replicates());
      takeSiteOffline(LON, NYC);
      assertOffline(LON, NYC);
      assertNoStateTransferInReceivingSite(NYC);
      assertNoStateTransferInSendingSite(LON);

      final OperationState operationState = new OperationState();
      final Object key = key(0);
      final Object initValue = value(0);
      final Object finalValue = value(1);
      final CheckPoint checkPoint = new CheckPoint();

      operation.init(cache(LON, 0), key, initValue, operationState);
      assertNotNull(operationState.initialValue);

      final BackupListener listener = new BackupListener() {
         @Override
         public void beforeCommand(VisitableCommand command, SiteAddress src) throws Exception {
            checkPoint.trigger("before-update");
            if (!performBeforeState && isUpdatingKeyWithValue(command, key, finalValue)) {
               //we need to wait for the state transfer before perform the command
               checkPoint.awaitStrict("update-key", 30, TimeUnit.SECONDS);
            }
         }

         @Override
         public void afterCommand(VisitableCommand command, SiteAddress src) throws Exception {
            if (performBeforeState && isUpdatingKeyWithValue(command, key, finalValue)) {
               //command was performed before state... let the state continue
               checkPoint.trigger("apply-state");
            }
         }

         @Override
         public void beforeState(XSiteStatePushCommand command, SiteAddress src) throws Exception {
            checkPoint.trigger("before-state");
            //wait until the command is received with the new value. so we make sure that the command saw the old value
            //and will commit a new value
            checkPoint.awaitStrict("before-update", 30, TimeUnit.SECONDS);
            if (performBeforeState && containsKey(command.getChunk(), key)) {
               //command before state... we need to wait
               checkPoint.awaitStrict("apply-state", 30, TimeUnit.SECONDS);
            }
         }

         @Override
         public void afterState(XSiteStatePushCommand command, SiteAddress src) throws Exception {
            if (!performBeforeState && containsKey(command.getChunk(), key)) {
               //state before command... let the command go...
               checkPoint.trigger("update-key");
            }
         }
      };

      for (CacheContainer cacheContainer : site(NYC).cacheManagers()) {
         BackupReceiverRepositoryWrapper.replaceInCache(cacheContainer, listener);
      }

      //safe (i.e. not blocking main thread), the state transfer is async
      startStateTransfer(LON, NYC);
      assertOnline(LON, NYC);

      //state transfer should send old value
      checkPoint.awaitStrict("before-state", 30, TimeUnit.SECONDS);


      //safe, perform is async
      operation.perform(cache(LON, 0), key, finalValue, initValue, operationState).get();

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return extractComponent(cache(LON, 0), XSiteAdminOperations.class).getRunningStateTransfer().isEmpty();
         }
      }, TimeUnit.SECONDS.toMillis(30));

      assertEventuallyNoStateTransferInReceivingSite(NYC, 30, TimeUnit.SECONDS);
      assertEventuallyNoStateTransferInSendingSite(LON, 30, TimeUnit.SECONDS);

      //check if all data is visible
      assertInSite(NYC, new AssertCondition<Object, Object>() {
         @Override
         public void assertInCache(Cache<Object, Object> cache) {
            assertEquals(operationState.finalValue, cache.get(key));
         }
      });
      assertInSite(LON, new AssertCondition<Object, Object>() {
         @Override
         public void assertInCache(Cache<Object, Object> cache) {
            assertEquals(operationState.finalValue, cache.get(key));
         }
      });
   }

   private void testConcurrentOperation(final Operation operation) throws Exception {
      assertNotNull(operation);
      assertTrue(operation.replicates());
      takeSiteOffline(LON, NYC);
      assertOffline(LON, NYC);
      assertNoStateTransferInReceivingSite(NYC);
      assertNoStateTransferInSendingSite(LON);

      final OperationState operationState = new OperationState();
      final Object key = key(0);
      final Object initValue = value(0);
      final Object finalValue = value(1);

      operation.init(cache(LON, 0), key, initValue, operationState);
      assertNull(operationState.initialValue);

      final XSiteStateProviderControl control = XSiteStateProviderControl.replaceInCache(cache(LON, 0));

      //safe (i.e. not blocking main thread), the state transfer is async
      final Thread thread = fork(new Runnable() {
         @Override
         public void run() {
            startStateTransfer(LON, NYC);
         }
      }, false);

      //state transfer will be running (nothing to transfer however) while the operation is done.
      control.await();
      assertOnline(LON, NYC);

      //safe, perform is async
      operation.perform(cache(LON, 0), key, finalValue, initValue, operationState).get();

      control.trigger();
      thread.join(TimeUnit.SECONDS.toMillis(30));

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return extractComponent(cache(LON, 0), XSiteAdminOperations.class).getRunningStateTransfer().isEmpty();
         }
      }, TimeUnit.SECONDS.toMillis(30));

      assertEventuallyNoStateTransferInReceivingSite(NYC, 30, TimeUnit.SECONDS);
      assertEventuallyNoStateTransferInSendingSite(LON, 30, TimeUnit.SECONDS);

      //check if all data is visible
      assertInSite(NYC, new AssertCondition<Object, Object>() {
         @Override
         public void assertInCache(Cache<Object, Object> cache) {
            assertEquals(operationState.finalValue, cache.get(key));
         }
      });
      assertInSite(LON, new AssertCondition<Object, Object>() {
         @Override
         public void assertInCache(Cache<Object, Object> cache) {
            assertEquals(operationState.finalValue, cache.get(key));
         }
      });
   }

   private void testStateTransferWithNoReplicatedOperation(final Operation operation) throws Exception {
      assertNotNull(operation);
      assertFalse(operation.replicates());
      takeSiteOffline(LON, NYC);
      assertOffline(LON, NYC);
      assertNoStateTransferInReceivingSite(NYC);
      assertNoStateTransferInSendingSite(LON);

      final OperationState operationState = new OperationState();
      final Object key = key(0);
      final Object initValue = value(0);
      final Object finalValue = value(1);
      final CheckPoint checkPoint = new CheckPoint();
      final AtomicBoolean commandReceived = new AtomicBoolean(false);

      operation.init(cache(LON, 0), key, initValue, operationState);
      assertNotNull(operationState.initialValue);

      final BackupListener listener = new BackupListener() {
         @Override
         public void beforeCommand(VisitableCommand command, SiteAddress src) throws Exception {
            commandReceived.set(true);
         }

         @Override
         public void afterCommand(VisitableCommand command, SiteAddress src) throws Exception {
            commandReceived.set(true);
         }

         @Override
         public void beforeState(XSiteStatePushCommand command, SiteAddress src) throws Exception {
            checkPoint.trigger("before-state");
            checkPoint.awaitStrict("before-update", 30, TimeUnit.SECONDS);
         }
      };

      for (CacheContainer cacheContainer : site(NYC).cacheManagers()) {
         BackupReceiverRepositoryWrapper.replaceInCache(cacheContainer, listener);
      }

      //safe (i.e. not blocking main thread), the state transfer is async
      startStateTransfer(LON, NYC);
      assertOnline(LON, NYC);

      //state transfer should send old value
      checkPoint.awaitStrict("before-state", 30, TimeUnit.SECONDS);

      //safe, perform is async
      operation.perform(cache(LON, 0), key, finalValue, initValue, operationState).get();

      assertFalse(commandReceived.get());
      checkPoint.trigger("before-update");

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return extractComponent(cache(LON, 0), XSiteAdminOperations.class).getRunningStateTransfer().isEmpty();
         }
      }, TimeUnit.SECONDS.toMillis(30));

      assertEventuallyNoStateTransferInReceivingSite(NYC, 30, TimeUnit.SECONDS);
      assertEventuallyNoStateTransferInSendingSite(LON, 30, TimeUnit.SECONDS);

      //check if all data is visible
      assertInSite(NYC, new AssertCondition<Object, Object>() {
         @Override
         public void assertInCache(Cache<Object, Object> cache) {
            assertEquals(operationState.finalValue, cache.get(key));
         }
      });
      assertInSite(LON, new AssertCondition<Object, Object>() {
         @Override
         public void assertInCache(Cache<Object, Object> cache) {
            assertEquals(operationState.finalValue, cache.get(key));
         }
      });
   }

   private boolean isUpdatingKeyWithValue(VisitableCommand command, Object key, Object value) {
      if (command instanceof PutKeyValueCommand) {
         return key.equals(((PutKeyValueCommand) command).getKey()) &&
               value.equals(((PutKeyValueCommand) command).getValue());
      } else if (command instanceof RemoveCommand) {
         return key.equals(((RemoveCommand) command).getKey());
      } else if (command instanceof ReplaceCommand) {
         return key.equals(((ReplaceCommand) command).getKey()) &&
               value.equals(((ReplaceCommand) command).getNewValue());
      } else if (command instanceof ClearCommand) {
         return true;
      } else if (command instanceof PutMapCommand) {
         return ((PutMapCommand) command).getMap().containsKey(key) &&
               ((PutMapCommand) command).getMap().get(key).equals(value);
      } else if (command instanceof PrepareCommand) {
         for (WriteCommand writeCommand : ((PrepareCommand) command).getModifications()) {
            if (isUpdatingKeyWithValue(writeCommand, key, value)) {
               return true;
            }
         }
      }
      return false;
   }

   private boolean containsKey(XSiteState[] states, Object key) {
      if (states == null || states.length == 0 || key == null) {
         return false;
      }
      for (XSiteState state : states) {
         if (key.equals(state.key())) {
            return true;
         }
      }
      return false;
   }

   private static enum Operation {
      PUT {
         @Override
         public <K, V> void init(Cache<K, V> cache, K key, V value, OperationState state) {
            cache.put(key, value);
            state.initialValue = value;
         }

         @Override
         public <K, V> Future<?> perform(Cache<K, V> cache, K key, V value, V oldValue, OperationState state) {
            state.finalValue = value;
            return cache.putAsync(key, value);
         }

         @Override
         public boolean replicates() {
            return true;
         }
      },
      PUT_IF_ABSENT {
         @Override
         public <K, V> void init(Cache<K, V> cache, K key, V value, OperationState state) {
            //no-op
            state.initialValue = null; //no key exists
         }

         @Override
         public <K, V> Future<?> perform(Cache<K, V> cache, K key, V value, V oldValue, OperationState state) {
            state.finalValue = value;
            return cache.putIfAbsentAsync(key, value);
         }

         @Override
         public boolean replicates() {
            return true;
         }
      },
      PUT_IF_ABSENT_FAIL {
         @Override
         public <K, V> void init(Cache<K, V> cache, K key, V value, OperationState state) {
            cache.put(key, value);
            state.initialValue = value;
         }

         @Override
         public <K, V> Future<?> perform(Cache<K, V> cache, K key, V value, V oldValue, OperationState state) {
            state.finalValue = oldValue;
            return cache.putIfAbsentAsync(key, value);
         }

         @Override
         public boolean replicates() {
            return false;
         }
      },
      REPLACE {
         @Override
         public <K, V> void init(Cache<K, V> cache, K key, V value, OperationState state) {
            cache.put(key, value);
            state.initialValue = value;
         }

         @Override
         public <K, V> Future<?> perform(Cache<K, V> cache, K key, V value, V oldValue, OperationState state) {
            state.finalValue = value;
            return cache.replaceAsync(key, value);
         }

         @Override
         public boolean replicates() {
            return true;
         }
      },
      /** not used: it has no state to transfer neither it is replicated! can be useful in the future. */
      REPLACE_NON_EXISTING {
         @Override
         public <K, V> void init(Cache<K, V> cache, K key, V value, OperationState state) {
            //no-op
            state.initialValue = null;
         }

         @Override
         public <K, V> Future<?> perform(Cache<K, V> cache, K key, V value, V oldValue, OperationState state) {
            state.finalValue = null;
            return cache.replaceAsync(key, value);
         }

         @Override
         public boolean replicates() {
            return false;
         }
      },
      REPLACE_IF_MATCH {
         @Override
         public <K, V> void init(Cache<K, V> cache, K key, V value, OperationState state) {
            cache.put(key, value);
            state.initialValue = value;
         }

         @Override
         public <K, V> Future<?> perform(Cache<K, V> cache, K key, V value, V oldValue, OperationState state) {
            state.finalValue = value;
            return cache.replaceAsync(key, oldValue, value);
         }

         @Override
         public boolean replicates() {
            return true;
         }
      },
      REPLACE_IF_MATCH_FAIL {
         @Override
         public <K, V> void init(Cache<K, V> cache, K key, V value, OperationState state) {
            cache.put(key, value);
            state.initialValue = value;
         }

         @Override
         public <K, V> Future<?> perform(Cache<K, V> cache, K key, V value, V oldValue, OperationState state) {
            state.finalValue = state.initialValue;
            if (value.equals(state.initialValue)) {
               throw new AssertionError("We need a different value");
            }
            //this works because value != initial value, so the match will fail.
            return cache.replaceAsync(key, value, value);
         }

         @Override
         public boolean replicates() {
            return false;
         }
      },
      REMOVE_NON_EXISTING {
         @Override
         public <K, V> void init(Cache<K, V> cache, K key, V value, OperationState state) {
            //no-op
            state.initialValue = null;
         }

         @Override
         public <K, V> Future<?> perform(Cache<K, V> cache, K key, V value, V oldValue, OperationState state) {
            state.finalValue = null;
            return cache.removeAsync(key);
         }

         @Override
         public boolean replicates() {
            return true;
         }
      },
      REMOVE {
         @Override
         public <K, V> void init(Cache<K, V> cache, K key, V value, OperationState state) {
            cache.put(key, value);
            state.initialValue = value;
         }

         @Override
         public <K, V> Future<?> perform(Cache<K, V> cache, K key, V value, V oldValue, OperationState state) {
            state.finalValue = null;
            return cache.removeAsync(key);
         }

         @Override
         public boolean replicates() {
            return true;
         }
      },
      REMOVE_IF_MATCH {
         @Override
         public <K, V> void init(Cache<K, V> cache, K key, V value, OperationState state) {
            cache.put(key, value);
            state.initialValue = value;
         }

         @Override
         public <K, V> Future<?> perform(Cache<K, V> cache, K key, V value, V oldValue, OperationState state) {
            state.finalValue = null;
            return cache.removeAsync(key, oldValue);
         }

         @Override
         public boolean replicates() {
            return true;
         }
      },
      REMOVE_IF_MATCH_FAIL {
         @Override
         public <K, V> void init(Cache<K, V> cache, K key, V value, OperationState state) {
            state.initialValue = value;
            cache.put(key, value);
         }

         @Override
         public <K, V> Future<?> perform(Cache<K, V> cache, K key, V value, V oldValue, OperationState state) {
            state.finalValue = state.initialValue;
            if (value.equals(state.initialValue)) {
               throw new AssertionError("We need a different value");
            }
            //this works because value != initial value, so the match will fail.
            return cache.removeAsync(key, value);
         }

         @Override
         public boolean replicates() {
            return false;
         }
      },
      CLEAR {
         @Override
         public <K, V> void init(Cache<K, V> cache, K key, V value, OperationState state) {
            state.initialValue = value;
            cache.put(key, value);
         }

         @Override
         public <K, V> Future<?> perform(Cache<K, V> cache, K key, V value, V oldValue, OperationState state) {
            state.finalValue = null;
            return cache.clearAsync();
         }

         @Override
         public boolean replicates() {
            return true;
         }
      },
      PUT_MAP {
         @Override
         public <K, V> void init(Cache<K, V> cache, K key, V value, OperationState state) {
            state.initialValue = value;
            cache.put(key, value);
         }

         @Override
         public <K, V> Future<?> perform(Cache<K, V> cache, K key, V value, V oldValue, OperationState state) {
            state.finalValue = value;
            Map<K, V> map = new HashMap<K, V>();
            map.put(key, value);
            return cache.putAllAsync(map);
         }

         @Override
         public boolean replicates() {
            return true;
         }
      };

      public abstract <K, V> void init(Cache<K, V> cache, K key, V value, OperationState state);

      public abstract <K, V> Future<?> perform(Cache<K, V> cache, K key, V value, V oldValue, OperationState state);

      public abstract boolean replicates();
   }

   private void startStateTransfer(String fromSite, String toSite) {
      XSiteAdminOperations operations = extractComponent(cache(fromSite, 0), XSiteAdminOperations.class);
      assertEquals(XSiteAdminOperations.SUCCESS, operations.pushState(toSite));
   }

   private void takeSiteOffline(String localSite, String remoteSite) {
      XSiteAdminOperations operations = extractComponent(cache(localSite, 0), XSiteAdminOperations.class);
      assertEquals(XSiteAdminOperations.SUCCESS, operations.takeSiteOffline(remoteSite));
   }

   private void assertOffline(String localSite, String remoteSite) {
      XSiteAdminOperations operations = extractComponent(cache(localSite, 0), XSiteAdminOperations.class);
      assertEquals(XSiteAdminOperations.OFFLINE, operations.siteStatus(remoteSite));
   }

   private void assertOnline(String localSite, String remoteSite) {
      XSiteAdminOperations operations = extractComponent(cache(localSite, 0), XSiteAdminOperations.class);
      assertEquals(XSiteAdminOperations.ONLINE, operations.siteStatus(remoteSite));
   }

   private int chunkSize(String site) {
      return cache(site, 0).getCacheConfiguration().sites().allBackups().get(0).stateTransfer().chunkSize();
   }

   private Object key(int index) {
      return "key-" + index;
   }

   private Object value(int index) {
      return "value-" + index;
   }

   private void assertNoStateTransferInReceivingSite(String siteName) {
      assertInSite(siteName, new AssertCondition<Object, Object>() {
         @Override
         public void assertInCache(Cache<Object, Object> cache) {
            KeyTrackAndCommitter keyTrackAndCommitter = extractComponent(cache, KeyTrackAndCommitter.class);
            assertFalse(keyTrackAndCommitter.isTracking(Flag.PUT_FOR_STATE_TRANSFER));
            assertFalse(keyTrackAndCommitter.isTracking(Flag.PUT_FOR_X_SITE_STATE_TRANSFER));
            assertTrue(keyTrackAndCommitter.isEmpty());
         }
      });
   }

   private void assertEventuallyNoStateTransferInReceivingSite(String siteName, long timeout, TimeUnit unit) {
      assertEventuallyInSite(siteName, new EventuallyAssertCondition<Object, Object>() {
         @Override
         public boolean assertInCache(Cache<Object, Object> cache) {
            KeyTrackAndCommitter keyTrackAndCommitter = extractComponent(cache, KeyTrackAndCommitter.class);
            return !keyTrackAndCommitter.isTracking(Flag.PUT_FOR_STATE_TRANSFER) &&
                  !keyTrackAndCommitter.isTracking(Flag.PUT_FOR_X_SITE_STATE_TRANSFER) &&
                  keyTrackAndCommitter.isEmpty();
         }
      }, unit.toMillis(timeout));
   }

   private void assertNoStateTransferInSendingSite(String siteName) {
      assertInSite(siteName, new AssertCondition<Object, Object>() {
         @Override
         public void assertInCache(Cache<Object, Object> cache) {
            assertTrue(extractComponent(cache, XSiteStateProvider.class).getCurrentStateSending().isEmpty());
         }
      });
   }

   private void assertEventuallyNoStateTransferInSendingSite(String siteName, long timeout, TimeUnit unit) {
      assertEventuallyInSite(siteName, new EventuallyAssertCondition<Object, Object>() {
         @Override
         public boolean assertInCache(Cache<Object, Object> cache) {
            return extractComponent(cache, XSiteStateProvider.class).getCurrentStateSending().isEmpty();
         }
      }, unit.toMillis(timeout));
   }

   private <K, V> void assertInSite(String siteName, AssertCondition<K, V> condition) {
      for (Cache<K, V> cache : this.<K, V>caches(siteName)) {
         condition.assertInCache(cache);
      }
   }

   private <K, V> void assertEventuallyInSite(final String siteName, final EventuallyAssertCondition<K, V> condition,
                                              long timeoutMillisecond) {
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            for (Cache<K, V> cache : BaseStateTransferTest.this.<K, V>caches(siteName)) {
               if (!condition.assertInCache(cache)) {
                  return false;
               }
            }
            return true;
         }
      }, timeoutMillisecond);
   }

   private interface AssertCondition<K, V> {
      void assertInCache(Cache<K, V> cache);
   }

   private interface EventuallyAssertCondition<K, V> {
      boolean assertInCache(Cache<K, V> cache);
   }

   private static class XSiteStateProviderControl extends XSiteProviderDelegate {

      private final CheckPoint checkPoint;

      private XSiteStateProviderControl(XSiteStateProvider xSiteStateProvider) {
         super(xSiteStateProvider);
         checkPoint = new CheckPoint();
      }

      @Override
      public void startStateTransfer(String siteName, Address requestor) {
         checkPoint.trigger("before-start");
         try {
            checkPoint.awaitStrict("await-start", 30, TimeUnit.SECONDS);
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
         } catch (TimeoutException e) {
            throw new RuntimeException(e);
         }
         super.startStateTransfer(siteName, requestor);
      }

      public final void await() throws TimeoutException, InterruptedException {
         checkPoint.awaitStrict("before-start", 30, TimeUnit.SECONDS);
      }

      public final void trigger() {
         checkPoint.trigger("await-start");
      }

      public static XSiteStateProviderControl replaceInCache(Cache<?, ?> cache) {
         XSiteStateProvider current = extractComponent(cache, XSiteStateProvider.class);
         XSiteStateProviderControl control = new XSiteStateProviderControl(current);
         replaceComponent(cache, XSiteStateProvider.class, control, true);
         return control;
      }
   }

   private static class BackupReceiverRepositoryWrapper extends BackupReceiverRepositoryDelegate {

      private final BackupListener listener;

      public BackupReceiverRepositoryWrapper(BackupReceiverRepository delegate, BackupListener listener) {
         super(delegate);
         if (listener == null) {
            throw new NullPointerException("Listener must not be null.");
         }
         this.listener = listener;
      }

      @Override
      public Object handleRemoteCommand(SingleRpcCommand cmd, SiteAddress src) throws Throwable {
         listener.beforeCommand((VisitableCommand) cmd.getCommand(), src);
         try {
            return super.handleRemoteCommand(cmd, src);
         } finally {
            listener.afterCommand((VisitableCommand) cmd.getCommand(), src);
         }
      }

      @Override
      public void handleStateTransferState(XSiteStatePushCommand cmd, SiteAddress src) throws Exception {
         listener.beforeState(cmd, src);
         try {
            super.handleStateTransferState(cmd, src);    // TODO: Customise this generated block
         } finally {
            listener.afterState(cmd, src);
         }
      }

      public static BackupReceiverRepositoryWrapper replaceInCache(CacheContainer cacheContainer, BackupListener listener) {
         BackupReceiverRepository delegate = extractGlobalComponent(cacheContainer, BackupReceiverRepository.class);
         BackupReceiverRepositoryWrapper wrapper = new BackupReceiverRepositoryWrapper(delegate, listener);
         replaceComponent(cacheContainer, BackupReceiverRepository.class, wrapper, true);
         JGroupsTransport t = (JGroupsTransport) extractGlobalComponent(cacheContainer, Transport.class);
         CommandAwareRpcDispatcher card = t.getCommandAwareRpcDispatcher();
         replaceField(wrapper, "backupReceiverRepository", card, CommandAwareRpcDispatcher.class);
         return wrapper;
      }
   }

   private static abstract class BackupListener {

      public void beforeCommand(VisitableCommand command, SiteAddress src) throws Exception {
         //no-op by default
      }

      public void afterCommand(VisitableCommand command, SiteAddress src) throws Exception {
         //no-op by default
      }

      public void beforeState(XSiteStatePushCommand command, SiteAddress src) throws Exception {
         //no-op by default
      }

      public void afterState(XSiteStatePushCommand command, SiteAddress src) throws Exception {
         //no-op by default
      }

   }

   private static class OperationState {
      private volatile Object initialValue;
      private volatile Object finalValue;
   }
}
