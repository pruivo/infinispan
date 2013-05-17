package org.infinispan.commands.control;

import org.infinispan.commands.LocalCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.context.InvocationContext;
import org.infinispan.lifecycle.ComponentStatus;

/**
 * @author Sebastiano Peluso
 */
public class ShadowTransactionCommand implements VisitableCommand, LocalCommand {


   public ShadowTransactionCommand() {
      //Yes this is empty
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitShadowTransactionCommand(ctx, this);
   }

   @Override
   public boolean shouldInvoke(InvocationContext ctx) {
      return true;
   }

   @Override
   public boolean ignoreCommandOnStatus(ComponentStatus status) {
      return false;
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      return null;
   }

   @Override
   public byte getCommandId() {
      return 0;
   }

   @Override
   public Object[] getParameters() {
      return null;
   }

   @Override
   public void setParameters(int commandId, Object[] parameters) {
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }
}
