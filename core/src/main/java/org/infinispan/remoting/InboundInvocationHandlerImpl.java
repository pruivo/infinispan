/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
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
package org.infinispan.remoting;

import org.infinispan.cacheviews.CacheViewsManager;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.control.CacheViewControlCommand;
import org.infinispan.commands.control.StateTransferControlCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.GMUCommitCommand;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.manager.NamedCacheNotFoundException;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.ResponseGenerator;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jgroups.blocks.RequestHandler;

import java.util.concurrent.ExecutorService;

/**
 * Sets the cache interceptor chain on an RPCCommand before calling it to perform
 *
 * @author Manik Surtani
 * @author Sebastiano Peluso
 * @since 4.0
 */
@Scope(Scopes.GLOBAL)
public class InboundInvocationHandlerImpl implements InboundInvocationHandler {
   GlobalComponentRegistry gcr;
   private static final Log log = LogFactory.getLog(InboundInvocationHandlerImpl.class);
   private static final boolean trace = log.isTraceEnabled();
   private EmbeddedCacheManager embeddedCacheManager;
   private GlobalConfiguration globalConfiguration;
   private Transport transport;
   private CacheViewsManager cacheViewsManager;
   private Configuration configuration;
   private ExecutorService asyncTransportExecutor;

   /**
    * How to handle an invocation based on the join status of a given cache *
    */
   private enum JoinHandle {
      OK, IGNORE
   }

   @Inject
   public void inject(GlobalComponentRegistry gcr,
                      EmbeddedCacheManager embeddedCacheManager, Transport transport,
                      GlobalConfiguration globalConfiguration, CacheViewsManager cacheViewsManager,
                      Configuration configuration
                      ,@ComponentName(KnownComponentNames.ASYNC_TRANSPORT_EXECUTOR) ExecutorService asyncTransportExecutor
                      ) {
      this.gcr = gcr;
      this.embeddedCacheManager = embeddedCacheManager;
      this.transport = transport;
      this.globalConfiguration = globalConfiguration;
      this.cacheViewsManager = cacheViewsManager;
      this.configuration = configuration;
      this.asyncTransportExecutor = asyncTransportExecutor;
   }

   private boolean hasJoinStarted(final ComponentRegistry componentRegistry) throws InterruptedException {
      StateTransferManager stateTransferManager = componentRegistry.getStateTransferManager();
      return stateTransferManager == null || stateTransferManager.hasJoinStarted();
   }

   @Override
   public Object handle(final CacheRpcCommand cmd, Address origin) throws Throwable {
      cmd.setOrigin(origin);

      // TODO Support global commands separately
      if (cmd instanceof CacheViewControlCommand) {
         ((CacheViewControlCommand) cmd).init(cacheViewsManager);
         try {
            return SuccessfulResponse.create(cmd.perform(null));
         } catch (Exception e) {
            return new ExceptionResponse(e);
         }
      }

      String cacheName = cmd.getCacheName();
      ComponentRegistry cr = gcr.getNamedComponentRegistry(cacheName);      
      
      if (cr == null) {
         if (!globalConfiguration.isStrictPeerToPeer()) {
            if (trace) log.tracef("Strict peer to peer off, so silently ignoring that %s cache is not defined", cacheName);
            return null;
         }

         log.namedCacheDoesNotExist(cacheName);
         return new ExceptionResponse(new NamedCacheNotFoundException(cacheName, "Cache has not been started on node " + transport.getAddress()));
      }

      return handleWithRetry(cmd, cr);
   }


   private Object handleInternal(final CacheRpcCommand cmd, final ComponentRegistry cr) throws Throwable {
      CommandsFactory commandsFactory = cr.getCommandsFactory();

      // initialize this command with components specific to the intended cache instance
      commandsFactory.initializeReplicableCommand(cmd, true);

      boolean threadCanBlock = this.configuration.locking().isolationLevel().equals(IsolationLevel.SERIALIZABLE) && !this.configuration.transaction().syncCommitPhase();
      if(!threadCanBlock || !(cmd instanceof CommitCommand)){
         try {
            if (trace) log.tracef("Calling perform() on %s", cmd);
            ResponseGenerator respGen = cr.getResponseGenerator();
            cmd.setResponseGenerator(respGen);
            Object retval = cmd.perform(null);
            if (retval == RequestHandler.DO_NOT_REPLY) {
               return retval;
            }
            return respGen.getResponse(cmd, retval);
         } catch (Exception e) {
            log.trace("Exception executing command", e);
            return new ExceptionResponse(e);
         }
      }else{
          this.asyncTransportExecutor.execute(new Runnable() {
             @Override
             public void run() {

                try {
                   if (trace) log.tracef("Calling perform() on %s from a new thread", cmd);
                   ResponseGenerator respGen = cr.getResponseGenerator();
                   cmd.setResponseGenerator(respGen);
                   Object retval = cmd.perform(null);

                } catch (Throwable e) {
                   log.trace("Exception executing command", e);

                }
             }
          });
         return RequestHandler.DO_NOT_REPLY;
      }
   }

   private Object handleWithWaitForBlocks(final CacheRpcCommand cmd, final ComponentRegistry cr) throws Throwable {
      return handleInternal(cmd, cr);
   }

   private Object handleWithRetry(final CacheRpcCommand cmd, final ComponentRegistry componentRegistry) throws Throwable {
      // RehashControlCommands are the mechanism used for joining the cluster,
      // so they don't need to wait until the cache starts up.
      boolean isStateTransferCommand = cmd instanceof StateTransferControlCommand;
      if (!isStateTransferCommand) {
         // For normal commands, reject them if we didn't start joining yet
         if (!hasJoinStarted(componentRegistry)) {
            log.cacheCanNotHandleInvocations(cmd.getCacheName());
            return new ExceptionResponse(new NamedCacheNotFoundException(cmd.getCacheName(),
                                                                         "Cache has not been started on node " + transport.getAddress()));
         }
         // if we did start joining, the StateTransferLockInterceptor will make it wait until the state transfer is complete
         // TODO There is a small window between starting the join and blocking the transactions, we need to eliminate it
         //waitForStart(cmd.getComponentRegistry());
      }
      return handleWithWaitForBlocks(cmd, componentRegistry);
   }
}

