/*
 * INESC-ID, Instituto de Engenharia de Sistemas e Computadores Investigação e Desevolvimento em Lisboa
 * Copyright 2013 INESC-ID and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 3.0 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
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
