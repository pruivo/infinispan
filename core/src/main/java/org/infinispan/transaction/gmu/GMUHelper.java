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
package org.infinispan.transaction.gmu;

import org.infinispan.CacheException;
import org.infinispan.commands.tx.GMUPrepareCommand;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.gmu.InternalGMUCacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.InequalVersionComparisonResult;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.container.versioning.gmu.GMUVersion;
import org.infinispan.container.versioning.gmu.GMUVersionGenerator;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class GMUHelper {

   private static final Log log = LogFactory.getLog(GMUHelper.class);

   public static void performReadSetValidation(GMUPrepareCommand prepareCommand,
                                               DataContainer dataContainer,
                                               ClusteringDependentLogic keyLogic) {
      GlobalTransaction gtx = prepareCommand.getGlobalTransaction();
      if (prepareCommand.getReadSet() == null || prepareCommand.getReadSet().length == 0) {
         if (log.isDebugEnabled()) {
            log.debugf("Validation of [%s] OK. no read set", gtx.prettyPrint());
         }
         return;
      }
      EntryVersion prepareVersion = prepareCommand.getPrepareVersion();
      for (Object key : prepareCommand.getReadSet()) {
         if (keyLogic.localNodeIsOwner(key)) {
            InternalCacheEntry cacheEntry = dataContainer.get(key, null); //get the most recent
            EntryVersion currentVersion = cacheEntry.getVersion();
            if (log.isDebugEnabled()) {
               log.debugf("[%s] Validate [%s]: Compare %s vs %s", gtx.prettyPrint(), key, currentVersion, prepareVersion);
            }
            if (currentVersion == null) {
               //this should only happens if the key does not exits. However, this can create some
               //consistency issues when eviction is enabled
               continue;
            }
            if (currentVersion.compareTo(prepareVersion) == InequalVersionComparisonResult.AFTER) {
               throw new ValidationException("Validation failed for key [" + key + "]", key);
            }
         } else {
            if (log.isDebugEnabled()) {
               log.debugf("[%s] Validate [%s]: keys is not local", gtx.prettyPrint(), key);
            }
         }
      }
   }

   public static EntryVersion calculateCommitVersion(EntryVersion mergedVersion, GMUVersionGenerator versionGenerator,
                                                     Collection<Address> affectedOwners) {
      if (mergedVersion == null) {
         throw new NullPointerException("Null merged version is not allowed to calculate commit view");
      }

      return versionGenerator.calculateCommitVersion(mergedVersion, affectedOwners);
   }

   public static InternalGMUCacheEntry toInternalGMUCacheEntry(InternalCacheEntry entry) {
      return convert(entry, InternalGMUCacheEntry.class);
   }

   public static GMUVersion toGMUVersion(EntryVersion version) {
      return convert(version, GMUVersion.class);
   }

   public static GMUVersionGenerator toGMUVersionGenerator(VersionGenerator versionGenerator) {
      return convert(versionGenerator, GMUVersionGenerator.class);
   }

   public static <T> T convert(Object object, Class<T> clazz) {
      if (log.isDebugEnabled()) {
         log.debugf("Convert object %s to class %s", object, clazz.getCanonicalName());
      }
      try {
         return clazz.cast(object);
      } catch (ClassCastException cce) {
         log.fatalf(cce, "Error converting object %s to class %s", object, clazz.getCanonicalName());
         throw new IllegalArgumentException("Expected " + clazz.getSimpleName() +
                                                  " and not " + object.getClass().getSimpleName());
      }
   }

   public static void joinAndSetTransactionVersion(Collection<Response> responses, TxInvocationContext ctx,
                                                   GMUVersionGenerator versionGenerator) {
      if (responses.isEmpty()) {
         if (log.isDebugEnabled()) {
            log.debugf("Versions received are empty!");
         }
         return;
      }
      List<EntryVersion> allPreparedVersions = new LinkedList<EntryVersion>();
      allPreparedVersions.add(ctx.getTransactionVersion());
      GlobalTransaction gtx = ctx.getGlobalTransaction();

      //process all responses
      for (Response r : responses) {
         if (r == null) {
            throw new IllegalStateException("Non-null response with new version is expected");
         } else if (r instanceof SuccessfulResponse) {
            EntryVersion version = convert(((SuccessfulResponse) r).getResponseValue(), EntryVersion.class);
            allPreparedVersions.add(version);
         } else if(r instanceof ExceptionResponse) {
            throw new ValidationException(((ExceptionResponse) r).getException());
         } else if(!r.isSuccessful()) {
            throw new CacheException("Unsuccessful response received... aborting transaction " + gtx.prettyPrint());
         }
      }

      EntryVersion[] preparedVersionsArray = new EntryVersion[allPreparedVersions.size()];
      EntryVersion commitVersion = versionGenerator.mergeAndMax(allPreparedVersions.toArray(preparedVersionsArray));

      if (log.isTraceEnabled()) {
         log.tracef("Merging transaction [%s] prepare versions %s ==> %s", gtx.prettyPrint(), allPreparedVersions,
                    commitVersion);
      }

      ctx.setTransactionVersion(commitVersion);
   }
}
