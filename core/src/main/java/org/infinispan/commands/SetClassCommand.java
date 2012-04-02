package org.infinispan.commands;


import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.lifecycle.ComponentStatus;

import java.util.Set;

/**
 * User: Fabio Mariotti
 * Date: 30/04/12
 */
public class SetClassCommand implements VisitableCommand{

    //The ID has been manually generated, 101 has been not used yet.
    public static final byte COMMAND_ID = 99;

    private String transactionalClass;

    public SetClassCommand(String transactionalClass) {
        this.transactionalClass = transactionalClass;
    }

    public String getTransactionalClass() {
        return transactionalClass;
    }

    @Override
    public Object perform(InvocationContext ctx) throws Throwable {
        return null;
    }

    @Override
    public byte getCommandId() {
        return COMMAND_ID;
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{transactionalClass};
    }

    @Override
    public void setParameters(int commandId, Object[] parameters) {
        if (commandId != COMMAND_ID) throw new IllegalStateException("Invalid method id");
        transactionalClass = (String) parameters[0];

    }

    @Override
    public boolean isReturnValueExpected() {
        return false;
    }

    @Override
    public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
        return visitor.visitSetClassCommand(ctx, this);
    }

    @Override
    public boolean shouldInvoke(InvocationContext ctx) {
        return false;
    }

    @Override
    public boolean ignoreCommandOnStatus(ComponentStatus status) {
        return false;
    }
}
