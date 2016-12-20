package org.infinispan.remoting.responses;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.marshall.core.Ids;

/**
 * //TODO document this!
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class PrimaryWriteResponse implements Response {

   public static final AdvancedExternalizer<PrimaryWriteResponse> EXTERNALIZER = new Externalizer();
   private final boolean successful;
   private final Object returnValue;

   public PrimaryWriteResponse(boolean successful, Object returnValue) {
      this.successful = successful;
      this.returnValue = returnValue;
   }

   @Override
   public boolean isSuccessful() {
      return true;
   }

   @Override
   public boolean isValid() {
      return true;
   }

   public boolean isCommandSuccessful() {
      return successful;
   }

   public Object getReturnValue() {
      return returnValue;
   }


   private static class Externalizer implements AdvancedExternalizer<PrimaryWriteResponse> {

      @Override
      public Set<Class<? extends PrimaryWriteResponse>> getTypeClasses() {
         return Collections.singleton(PrimaryWriteResponse.class);
      }

      @Override
      public Integer getId() {
         return Ids.PRIMARY_WRITE_RESPONSE;
      }

      @Override
      public void writeObject(ObjectOutput output, PrimaryWriteResponse object) throws IOException {
         output.writeBoolean(object.successful);
         output.writeObject(object.returnValue);
      }

      @Override
      public PrimaryWriteResponse readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new PrimaryWriteResponse(input.readBoolean(), input.readObject());
      }
   }
}
