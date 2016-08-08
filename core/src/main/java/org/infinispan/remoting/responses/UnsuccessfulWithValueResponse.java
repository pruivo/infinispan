package org.infinispan.remoting.responses;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.marshall.core.Ids;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public class UnsuccessfulWithValueResponse extends ValidResponse {

   private final Object returnValue;

   private UnsuccessfulWithValueResponse(Object returnValue) {
      this.returnValue = returnValue;
   }

   @Override
   public boolean isSuccessful() {
      return false;
   }

   public Object getReturnValue() {
      return returnValue;
   }

   public static UnsuccessfulWithValueResponse create(Object returnValue) {
      return new UnsuccessfulWithValueResponse(returnValue);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      UnsuccessfulWithValueResponse that = (UnsuccessfulWithValueResponse) o;

      return returnValue != null ? returnValue.equals(that.returnValue) : that.returnValue == null;

   }

   @Override
   public int hashCode() {
      return returnValue != null ? returnValue.hashCode() : 0;
   }

   public static class Externalizer extends AbstractExternalizer<UnsuccessfulWithValueResponse> {

      @Override
      public Set<Class<? extends UnsuccessfulWithValueResponse>> getTypeClasses() {
         return Collections.singleton(UnsuccessfulWithValueResponse.class);
      }

      @Override
      public void writeObject(ObjectOutput output, UnsuccessfulWithValueResponse object) throws IOException {
         output.writeObject(object.returnValue);
      }

      @Override
      public UnsuccessfulWithValueResponse readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new UnsuccessfulWithValueResponse(input.readObject());
      }

      @Override
      public Integer getId() {
         return Ids.UNSUCCESSFUL_WITH_VALUE_RESPONSE;
      }
   }
}
