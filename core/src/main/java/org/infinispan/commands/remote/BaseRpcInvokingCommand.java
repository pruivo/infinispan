package org.infinispan.commands.remote;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commons.util.FastCopyHashMap;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Map;

/**
 * Base class for RPC commands.
 *
 * @author Mircea.Markus@jboss.com
 */
public abstract class BaseRpcInvokingCommand extends BaseRpcCommand {

   protected InterceptorChain interceptorChain;
   protected InvocationContextFactory icf;
   private final transient Map<VisitableCommand, InvocationContext> contextMap;

   private static final Log log = LogFactory.getLog(BaseRpcInvokingCommand.class);
   private static final boolean trace = log.isTraceEnabled();

   protected BaseRpcInvokingCommand(String cacheName) {
      super(cacheName);
      contextMap = new FastCopyHashMap<VisitableCommand, InvocationContext>();
   }

   public void init(InterceptorChain interceptorChain, InvocationContextFactory icf) {
      this.interceptorChain = interceptorChain;
      this.icf = icf;
   }

   protected final Object processVisitableCommand(ReplicableCommand cacheCommand) throws Throwable {
      if (cacheCommand instanceof VisitableCommand) {
         VisitableCommand vc = (VisitableCommand) cacheCommand;
         final InvocationContext ctx = createInvocationContextIfAbsent(vc);
         if (vc.shouldInvoke(ctx)) {
            if (trace) log.tracef("Invoking command %s, with originLocal flag set to %b", cacheCommand, ctx.isOriginLocal());
            return interceptorChain.invoke(ctx, vc);
         } else {
            if (trace) log.tracef("Not invoking command %s since shouldInvoke() returned false with context %s", cacheCommand, ctx);
            return null;
         }
         // we only need to return values for a set of remote calls; not every call.
      } else {
         throw new RuntimeException("Do we still need to deal with non-visitable commands? (" + cacheCommand.getClass().getName() + ")");
      }
   }

   public InvocationContext createInvocationContextIfAbsent(VisitableCommand command) {
      InvocationContext ctx = contextMap.get(command);
      if (ctx == null) {
         ctx = icf.createRemoteInvocationContextForCommand(command, getOrigin());
         contextMap.put(command, ctx);
      }
      return ctx;
   }
}
