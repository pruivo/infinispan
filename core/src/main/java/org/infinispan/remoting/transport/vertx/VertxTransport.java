package org.infinispan.remoting.transport.vertx;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.inboundhandler.GlobalInboundInvocationHandler;
import org.infinispan.remoting.inboundhandler.InboundInvocationHandler;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.BackupResponse;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.XSiteResponse;
import org.infinispan.remoting.transport.impl.EmptyRaftManager;
import org.infinispan.remoting.transport.raft.RaftManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.XSiteBackup;
import org.infinispan.xsite.commands.remote.XSiteRequest;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.grpc.client.GrpcClient;
import io.vertx.grpc.server.GrpcServer;
import io.vertx.grpc.server.GrpcServerRequest;


public class VertxTransport implements Transport {

   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   // TODO configure?
   private static final String BIND_HOST = "127.0.0.1";
   // TODO configure? 0 == random
   private static final int BIND_PORT = 0;

   private final Vertx vertx;
   private final GrpcServer rpcServer;
   private final GrpcClient rpcClient;

   private HttpServer httpServer;
   private VertxAddress local;

   InboundInvocationHandler handler;
   StreamingMarshaller marshaller;


   public VertxTransport() {
      vertx = Vertx.vertx();
      rpcServer = GrpcServer.server(vertx);
      rpcClient = GrpcClient.client(vertx);
      rpcServer.callHandler(InfinispanGrpc.getInvokeCommandMethod(), this::onCommandReceived);
   }

   private void onCommandReceived(GrpcServerRequest<Remoting.Command, Remoting.Response> request) {
      request.handler(command -> {
         ReplicableCommand cmd = null;
         try {
            cmd = (ReplicableCommand) marshaller.objectFromInputStream(command.getCmd().newInput());
         } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
         }
         handler.handleFromCluster(null, cmd, response -> {}, DeliverOrder.NONE);
         request.response().send(Remoting.Response.newBuilder().setMessage().build())
      });
   }

   @Override
   public void start() {
      if (httpServer != null) {
         httpServer.close();
      }
      httpServer = vertx.createHttpServer()
            .requestHandler(rpcServer)
            .listen(BIND_PORT, BIND_HOST)
            .result();
      local = VertxAddress.createRandom(BIND_HOST, httpServer.actualPort());
   }

   @Override
   public void stop() {

   }


   @Override
   public CompletableFuture<Map<Address, Response>> invokeRemotelyAsync(Collection<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout, ResponseFilter responseFilter, DeliverOrder deliverOrder, boolean anycast) throws Exception {
      return null;
   }

   @Override
   public void sendTo(Address destination, ReplicableCommand rpcCommand, DeliverOrder deliverOrder) throws Exception {

   }

   @Override
   public void sendToMany(Collection<Address> destinations, ReplicableCommand rpcCommand, DeliverOrder deliverOrder) throws Exception {

   }

   @Override
   public BackupResponse backupRemotely(Collection<XSiteBackup> backups, XSiteRequest<?> rpcCommand) throws Exception {
      return null;
   }

   @Override
   public <O> XSiteResponse<O> backupRemotely(XSiteBackup backup, XSiteRequest<O> rpcCommand) {
      return null;
   }

   @Override
   public boolean isCoordinator() {
      return false;
   }

   @Override
   public Address getCoordinator() {
      return null;
   }

   @Override
   public Address getAddress() {
      return null;
   }

   @Override
   public List<Address> getPhysicalAddresses() {
      return null;
   }

   @Override
   public List<Address> getMembers() {
      return null;
   }

   @Override
   public List<Address> getMembersPhysicalAddresses() {
      return null;
   }

   @Override
   public boolean isMulticastCapable() {
      return false;
   }

   @Override
   public void checkCrossSiteAvailable() throws CacheConfigurationException {

   }

   @Override
   public String localSiteName() {
      return null;
   }


   @Override
   public int getViewId() {
      return 0;
   }

   @Override
   public CompletableFuture<Void> withView(int expectedViewId) {
      return null;
   }

   @Override
   public void waitForView(int viewId) throws InterruptedException {

   }

   @Override
   public Log getLog() {
      return log;
   }

   @Override
   public Set<String> getSitesView() {
      return Collections.emptySet();
   }

   @Override
   public boolean isSiteCoordinator() {
      return false;
   }

   @Override
   public Collection<Address> getRelayNodesAddress() {
      return Collections.emptyList();
   }

   @Override
   public RaftManager raftManager() {
      return EmptyRaftManager.INSTANCE;
   }
}
