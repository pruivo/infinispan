/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE. See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA 02110-1301, USA.
 */

package org.infinispan.interceptors.locking;

import org.infinispan.commands.tx.GMUPrepareCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.transaction.gmu.GMUHelper;
import org.infinispan.util.TimSort;

/**
 * @author Pedro Ruivo
 * @since 5.2
 */
public class OptimisticReadWriteLockingInterceptor extends OptimisticLockingInterceptor {

   @Override
   protected void afterWriteLocksAcquired(TxInvocationContext ctx, PrepareCommand command) throws InterruptedException {
      GMUPrepareCommand spc = GMUHelper.convert(command, GMUPrepareCommand.class);
      Object[] readSet = spc.getReadSet();
      TimSort.sort(readSet, PrepareCommand.KEY_COMPARATOR);
      acquireReadLocks(ctx, readSet);
   }

   private void acquireReadLocks(TxInvocationContext ctx, Object[] readSet) throws InterruptedException {
      long lockTimeout = cacheConfiguration.locking().lockAcquisitionTimeout();
      for (Object key : readSet) {
         lockAndRegisterShareBackupLock(ctx, key, lockTimeout, false);
         ctx.addAffectedKey(key);
      }
   }
}
