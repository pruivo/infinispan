package org.infinispan.commands.read;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;

import java.util.Set;

import static org.infinispan.container.DataContainer.AccessMode;

/**
 * Command to calculate the size of the cache
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Mircea.Markus@jboss.com
 * @author <a href="http://gleamynode.net/">Trustin Lee</a>
 * @since 4.0
 */
public class SizeCommand extends AbstractLocalCommand implements VisitableCommand {
   private final DataContainer container;

   public SizeCommand(DataContainer container, Set<Flag> flags) {
      setFlags(flags);
      this.container = container;
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitSizeCommand(ctx, this);
   }

   @Override
   public Integer perform(InvocationContext ctx) throws Throwable {
      final AccessMode accessMode = accessMode();
      if (ctx.getLookedUpEntries().isEmpty()) {
         return container.size(accessMode);
      }

      int size = container.size(accessMode);
      for (CacheEntry e: ctx.getLookedUpEntries().values()) {
         if (container.containsKey(e.getKey(), accessMode)) {
            if (e.isRemoved()) {
               size --;
            }
         } else if (!e.isRemoved()) {
            size ++;
         }
      }

      return Math.max(size, 0);
   }

   @Override
   public String toString() {
      return "SizeCommand{" +
            "containerSize=" + container.size(accessMode()) +
            '}';
   }

   private AccessMode accessMode() {
      return hasFlag(Flag.SKIP_CACHE_LOAD) ? AccessMode.SKIP_PERSISTENCE : AccessMode.ALL;
   }
}
