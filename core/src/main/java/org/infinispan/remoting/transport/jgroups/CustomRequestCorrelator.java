package org.infinispan.remoting.transport.jgroups;

import java.io.ByteArrayInputStream;

import org.infinispan.commons.marshall.StreamingMarshaller;
import org.jgroups.Address;
import org.jgroups.blocks.Request;
import org.jgroups.blocks.RequestCorrelator;
import org.jgroups.blocks.RequestHandler;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;

/**
 * Extend {@link RequestCorrelator} to use our own marshaller.
 *
 * @author Dan Berindei
 * @since 9.0
 */
class CustomRequestCorrelator extends RequestCorrelator {
   private StreamingMarshaller ispnMarshaller;

   public CustomRequestCorrelator(Protocol transport, RequestHandler handler, Address local_addr,
                                  StreamingMarshaller ispnMarshaller) {
      super(ClassConfigurator.getProtocolId(RequestCorrelator.class), transport, handler, local_addr);
      this.ispnMarshaller = ispnMarshaller;
   }

   @Override
   protected void handleResponse(Request req, Address sender, byte[] buf, int offset, int length,
                                 boolean is_exception) {
      Object retval;
      try {
         ByteArrayInputStream is = new ByteArrayInputStream(buf, offset, length);
         retval = ispnMarshaller.objectFromInputStream(is);
      } catch (Exception e) {
         log.error(Util.getMessage("FailedUnmarshallingBufferIntoReturnValue"), e);
         retval = e;
         is_exception = true;
      }
      req.receiveResponse(retval, sender, is_exception);
   }
}
