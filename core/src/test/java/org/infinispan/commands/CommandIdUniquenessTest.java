package org.infinispan.commands;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.infinispan.commands.irac.IracCleanupKeysCommand;
import org.infinispan.commands.irac.IracMetadataRequestCommand;
import org.infinispan.commands.irac.IracStateResponseCommand;
import org.infinispan.commands.irac.IracTombstoneCleanupCommand;
import org.infinispan.commands.irac.IracTombstonePrimaryCheckCommand;
import org.infinispan.commands.irac.IracTombstoneStateResponseCommand;
import org.infinispan.commands.irac.IracUpdateVersionCommand;
import org.infinispan.commands.remote.CheckTransactionRpcCommand;
import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.commands.topology.CacheAvailabilityUpdateCommand;
import org.infinispan.commands.topology.CacheJoinCommand;
import org.infinispan.commands.topology.CacheLeaveCommand;
import org.infinispan.commands.topology.CacheShutdownCommand;
import org.infinispan.commands.topology.CacheShutdownRequestCommand;
import org.infinispan.commands.topology.CacheStatusRequestCommand;
import org.infinispan.commands.topology.RebalancePhaseConfirmCommand;
import org.infinispan.commands.topology.RebalancePolicyUpdateCommand;
import org.infinispan.commands.topology.RebalanceStartCommand;
import org.infinispan.commands.topology.RebalanceStatusRequestCommand;
import org.infinispan.commands.topology.TopologyUpdateCommand;
import org.infinispan.commands.topology.TopologyUpdateStableCommand;
import org.infinispan.commons.util.ClassFinder;
import org.infinispan.manager.impl.ReplicableManagerFunctionCommand;
import org.infinispan.manager.impl.ReplicableRunnableCommand;
import org.infinispan.remoting.rpc.CustomReplicableCommand;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.Mocks;
import org.infinispan.topology.HeartBeatCommand;
import org.infinispan.xsite.commands.XSiteLocalEventCommand;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "commands.CommandIdUniquenessTest")
public class CommandIdUniquenessTest extends AbstractInfinispanTest {

   private static final Set<Integer> SKIP_TRACE;

   static {
      SortedSet<Integer> set = new TreeSet<>();
      // cache operations (not used data)
      set.add((int) CacheJoinCommand.COMMAND_ID);
      set.add((int) CacheAvailabilityUpdateCommand.COMMAND_ID);
      set.add((int) CacheShutdownRequestCommand.COMMAND_ID);
      set.add((int) CacheStatusRequestCommand.COMMAND_ID);
      set.add((int) TopologyUpdateCommand.COMMAND_ID);
      set.add((int) CacheLeaveCommand.COMMAND_ID);
      set.add((int) RebalanceStatusRequestCommand.COMMAND_ID);
      set.add((int) CacheShutdownCommand.COMMAND_ID);
      set.add((int) HeartBeatCommand.COMMAND_ID);
      set.add((int) RebalancePhaseConfirmCommand.COMMAND_ID);
      set.add((int) RebalancePolicyUpdateCommand.COMMAND_ID);
      set.add((int) RebalanceStartCommand.COMMAND_ID);
      set.add((int) CheckTransactionRpcCommand.COMMAND_ID);
      set.add((int) TopologyUpdateStableCommand.COMMAND_ID);

      // cluster executor
      set.add((int) ReplicableRunnableCommand.COMMAND_ID);
      set.add((int) ReplicableManagerFunctionCommand.COMMAND_ID);

      // async cross-site not traced (not in a critical path)
      set.add((int) IracUpdateVersionCommand.COMMAND_ID);
      set.add((int) IracTombstoneStateResponseCommand.COMMAND_ID);
      set.add((int) IracMetadataRequestCommand.COMMAND_ID);
      set.add((int) IracStateResponseCommand.COMMAND_ID);
      set.add((int) IracTombstoneCleanupCommand.COMMAND_ID);
      set.add((int) XSiteLocalEventCommand.COMMAND_ID);
      set.add((int) IracCleanupKeysCommand.COMMAND_ID);
      set.add((int) IracTombstonePrimaryCheckCommand.COMMAND_ID);

      // unknown?
      set.add((int) CustomReplicableCommand.COMMAND_ID);

      // wrapper, depends on the visitable command
      set.add((int) SingleRpcCommand.COMMAND_ID);

      SKIP_TRACE = set;
   }


   public void testCommandIdUniqueness() throws Exception {
      List<Class<?>> commands = ClassFinder.isAssignableFrom(ReplicableCommand.class);
      SortedMap<Integer, String> cmdIds = new TreeMap<>();
      var traceData = new TracedCommand.TraceCommandData(null, null);

      for (Class<?> c : commands) {
         if (!c.isInterface() && !Modifier.isAbstract(c.getModifiers()) && !LocalCommand.class.isAssignableFrom(c) && !c.getName().contains(Mocks.class.getName())) {
            log.infof("Testing %s", c.getSimpleName());
            Constructor<?>[] declaredCtors = c.getDeclaredConstructors();
            Constructor<?> constructor = null;
            for (Constructor<?> declaredCtor : declaredCtors) {
               if (declaredCtor.getParameterCount() == 0) {
                  constructor = declaredCtor;
                  constructor.setAccessible(true);
                  break;
               }
            }

            assertNotNull("Empty constructor not found for " + c.getSimpleName(), constructor);
            ReplicableCommand cmd = (ReplicableCommand) constructor.newInstance();
            int b = cmd.getCommandId();
            assertTrue("Command " + c.getSimpleName() + " has a command id of " + b + " and does not implement LocalCommand!", b > 0);
            assertFalse("Command ID [" + b + "] is duplicated in " + c.getSimpleName() + " and " + cmdIds.get(b), cmdIds.containsKey(b));

            if (!SKIP_TRACE.contains(b)) {
               cmd.setTraceCommandData(traceData);
               assertEquals("Command ID [" + b + "] " + c.getSimpleName() + " does not store trace information", traceData, cmd.getTraceCommandData());
            }

            cmdIds.put(b, c.getSimpleName());
         }
      }
   }
}
